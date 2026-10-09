//! Small shared helpers: clocks, timestamps and human-readable sizes.

use std::fs::Metadata;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};

/// Wall-clock seconds since the Unix epoch. The engine reads time through this
/// so tests can move it forward without sleeping.
pub trait Clock: Send + Sync {
    fn now(&self) -> f64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> f64 {
        now_secs()
    }
}

pub fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// POSIX seconds as RFC 3339 in UTC.
pub fn iso(timestamp: f64) -> String {
    let secs = timestamp.floor();
    let nanos = ((timestamp - secs) * 1e9).round().clamp(0.0, 999_999_999.0) as u32;
    DateTime::<Utc>::from_timestamp(secs as i64, nanos)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// A file's modification time in nanoseconds since the epoch.
pub fn mtime_ns(meta: &Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn human_bytes(nbytes: u64) -> String {
    let mut value = nbytes as f64;
    for unit in ["B", "KB", "MB", "GB"] {
        if value < 1000.0 {
            return if unit == "B" {
                format!("{value:.0} {unit}")
            } else {
                format!("{value:.1} {unit}")
            };
        }
        value /= 1000.0;
    }
    format!("{value:.1} TB")
}

pub fn human_duration(seconds: f64) -> String {
    let seconds = seconds.max(0.0) as u64;
    if seconds < 120 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 120 {
        return format!("{minutes}m");
    }
    let hours = minutes / 60;
    if hours < 48 {
        format!("{hours}h")
    } else {
        format!("{}d", hours / 24)
    }
}

/// The last `limit` bytes of `text`, on a character boundary.
pub fn tail(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut start = text.len() - limit;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_and_durations_read_naturally() {
        assert_eq!(human_bytes(4), "4 B");
        assert_eq!(human_bytes(1_250_000), "1.2 MB");
        assert_eq!(human_duration(61.0), "61s");
        assert_eq!(human_duration(7199.0), "119m");
        assert_eq!(human_duration(7200.0), "2h");
        assert_eq!(human_duration(3.0 * 86400.0), "3d");
    }

    #[test]
    fn iso_is_utc_with_millis() {
        assert_eq!(iso(0.5), "1970-01-01T00:00:00.500Z");
    }

    #[test]
    fn tail_respects_char_boundaries() {
        assert_eq!(tail("héllo", 4), "llo");
        assert_eq!(tail("abc", 10), "abc");
    }
}
