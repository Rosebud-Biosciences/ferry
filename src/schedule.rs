//! Cron schedules for `mode: cron` pipelines, evaluated in the pipeline's
//! `timezone` (an IANA name) or, without one, the machine's.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Local, Utc};
use croner::Cron;
use tokio_util::sync::CancellationToken;

/// The longest single nap while waiting for a run: a wall-clock change or a
/// machine waking from sleep is noticed within this long.
const MAX_NAP: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy)]
pub enum Zone {
    Local,
    Named(chrono_tz::Tz),
}

#[derive(Debug, Clone)]
pub struct Schedule {
    pub expr: String,
    cron: Cron,
    zone: Zone,
}

impl Schedule {
    pub fn new(expr: &str, timezone: Option<&str>) -> Result<Self> {
        let cron = Cron::from_str(expr).map_err(|e| anyhow!("invalid cron {expr:?}: {e}"))?;
        let zone = match timezone {
            None => Zone::Local,
            Some(name) => Zone::Named(name.parse().map_err(|_| anyhow!("unknown timezone {name:?}"))?),
        };
        Ok(Self {
            expr: expr.to_string(),
            cron,
            zone,
        })
    }

    /// The first run strictly after `after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let next = match self.zone {
            Zone::Local => self
                .cron
                .find_next_occurrence(&after.with_timezone(&Local), false)
                .map(|t| t.with_timezone(&Utc)),
            Zone::Named(tz) => self
                .cron
                .find_next_occurrence(&after.with_timezone(&tz), false)
                .map(|t| t.with_timezone(&Utc)),
        };
        next.map_err(|e| anyhow!("cron {:?} has no next run: {e}", self.expr))
    }

    /// The next `n` runs after `after`.
    pub fn upcoming(&self, after: DateTime<Utc>, n: usize) -> Vec<DateTime<Utc>> {
        let mut out = Vec::with_capacity(n);
        let mut t = after;
        while out.len() < n {
            match self.next_after(t) {
                Ok(next) => {
                    out.push(next);
                    t = next;
                }
                Err(_) => break,
            }
        }
        out
    }

    /// `t` as the schedule's zone shows it.
    pub fn show(&self, t: DateTime<Utc>) -> String {
        match self.zone {
            Zone::Local => t.with_timezone(&Local).format("%Y-%m-%d %H:%M %Z").to_string(),
            Zone::Named(tz) => t.with_timezone(&tz).format("%Y-%m-%d %H:%M %Z").to_string(),
        }
    }
}

/// Sleep until `when` by the wall clock; `false` if `stop` fired first.
pub async fn sleep_until(when: DateTime<Utc>, stop: &CancellationToken) -> bool {
    loop {
        let left = (when - Utc::now()).to_std().unwrap_or_default();
        if left.is_zero() {
            return true;
        }
        tokio::select! {
            _ = stop.cancelled() => return false,
            _ = tokio::time::sleep(left.min(MAX_NAP)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn runs_are_found_in_the_named_zone() {
        let nightly = Schedule::new("0 2 * * *", Some("Europe/Oslo")).unwrap();
        // 02:00 in Oslo is 00:00 UTC in summer and 01:00 UTC in winter.
        assert_eq!(
            nightly.next_after(at("2026-06-01T01:00:00Z")).unwrap(),
            at("2026-06-02T00:00:00Z")
        );
        assert_eq!(
            nightly.next_after(at("2026-12-01T01:00:00Z")).unwrap(),
            at("2026-12-02T01:00:00Z")
        );
    }

    #[test]
    fn a_repeated_hour_at_the_end_of_summer_time_runs_once() {
        let nightly = Schedule::new("30 1 * * *", Some("America/New_York")).unwrap();
        let days: Vec<String> = nightly
            .upcoming(at("2026-10-31T12:00:00Z"), 3)
            .into_iter()
            .map(|t| {
                t.with_timezone(&chrono_tz::America::New_York)
                    .format("%m-%d")
                    .to_string()
            })
            .collect();
        assert_eq!(days, ["11-01", "11-02", "11-03"]);
    }

    #[test]
    fn bad_expressions_and_zones_are_refused() {
        assert!(Schedule::new("0 25 * * *", None).is_err());
        assert!(Schedule::new("0 2 * * *", Some("Mars/Olympus")).is_err());
    }

    #[tokio::test]
    async fn a_stop_cuts_the_wait_short() {
        let stop = CancellationToken::new();
        stop.cancel();
        assert!(!sleep_until(Utc::now() + chrono::Duration::hours(1), &stop).await);
        assert!(sleep_until(Utc::now() - chrono::Duration::seconds(1), &CancellationToken::new()).await);
    }
}
