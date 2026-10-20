//! Stores: S3 (AWS, Ceph RGW, any S3 API) and local directories, behind
//! `object_store`. A [`Location`] is a store plus the key prefix it is rooted
//! at and the URI that prefix is shown (and registered) as.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::local::LocalFileSystem;
use object_store::path::Path as KeyPath;

use crate::config::StoreConfig;
use crate::transfer::RateLimiter;

pub struct Store {
    pub name: String,
    pub inner: Arc<dyn ObjectStore>,
    /// Key prefix inside the bucket (from `s3://bucket/prefix`).
    pub base: String,
    pub uri: String,
    pub limiter: Option<Arc<RateLimiter>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store({} at {})", self.name, self.uri)
    }
}

/// A configured prefix as key parts: either slash separates, and empty or
/// dot parts drop out.
pub fn normalize_prefix(prefix: &str) -> Result<String> {
    let parts: Vec<&str> = prefix
        .split(['/', '\\'])
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    if parts.contains(&"..") {
        bail!("prefix escapes its store: {prefix:?}");
    }
    Ok(parts.join("/"))
}

/// A `/`-separated path of file and directory names as an object key. Names
/// can hold anything but `/`, so each is percent-encoded where a key cannot
/// hold it faithfully -- `%`, `\` and control characters -- which keeps
/// distinct names distinct (`a\b` is not `a/b`) and decodes back exactly.
pub fn encode_rel(rel: &str) -> Result<String> {
    let mut parts = Vec::new();
    for part in rel.split('/').filter(|p| !p.is_empty()) {
        if part == "." || part == ".." {
            bail!("key escapes its prefix: {rel:?}");
        }
        let mut encoded = String::with_capacity(part.len());
        for c in part.chars() {
            if c == '%' || c == '\\' || c.is_control() {
                let mut buf = [0u8; 4];
                for byte in c.encode_utf8(&mut buf).bytes() {
                    encoded.push_str(&format!("%{byte:02X}"));
                }
            } else {
                encoded.push(c);
            }
        }
        parts.push(encoded);
    }
    Ok(parts.join("/"))
}

fn join(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, _) => b.to_string(),
        (_, true) => a.to_string(),
        _ => format!("{a}/{b}"),
    }
}

#[derive(Debug, Clone)]
pub struct Location {
    pub store: Arc<Store>,
    pub base: String,
    pub uri: String,
}

impl Location {
    pub fn root(store: Arc<Store>) -> Self {
        Self {
            base: store.base.clone(),
            uri: store.uri.clone(),
            store,
        }
    }

    /// The location under a configured prefix.
    pub fn child(&self, prefix: &str) -> Result<Self> {
        let rel = normalize_prefix(prefix)?;
        Ok(Self {
            store: self.store.clone(),
            base: join(&self.base, &rel),
            uri: if rel.is_empty() {
                self.uri.clone()
            } else {
                format!("{}/{rel}", self.uri)
            },
        })
    }

    /// The key of `rel`, a path of names under this location ([`encode_rel`]).
    pub fn key(&self, rel: &str) -> Result<KeyPath> {
        let full = join(&self.base, &encode_rel(rel)?);
        KeyPath::parse(&full).map_err(|e| anyhow!("{full:?} is not a valid object key: {e}"))
    }

    pub fn uri_for(&self, rel: &str) -> String {
        match encode_rel(rel) {
            Ok(rel) if !rel.is_empty() => format!("{}/{rel}", self.uri),
            _ => self.uri.clone(),
        }
    }
}

pub fn open_store(name: &str, cfg: &StoreConfig) -> Result<Arc<Store>> {
    let limiter = cfg.bandwidth.as_ref().map(|b| Arc::new(RateLimiter::new(b)));
    let (inner, base, uri): (Arc<dyn ObjectStore>, String, String) = if cfg.is_s3() {
        let rest = &cfg.url["s3://".len()..];
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            bail!("store {name}: no bucket in {:?}", cfg.url);
        }
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
        let mut region = cfg.region.clone();
        if let Some(profile) = &cfg.profile {
            let creds = aws_profile(profile).with_context(|| format!("store {name}: profile {profile}"))?;
            builder = builder
                .with_access_key_id(creds.access_key_id)
                .with_secret_access_key(creds.secret_access_key);
            if let Some(token) = creds.session_token {
                builder = builder.with_token(token);
            }
            region = region.or(creds.region);
        }
        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &cfg.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        builder = builder
            .with_virtual_hosted_style_request(cfg.virtual_hosted_style)
            .with_allow_http(cfg.allow_http);
        if let Some(checksum) = &cfg.checksum {
            let algorithm = checksum
                .parse::<object_store::aws::Checksum>()
                .map_err(|_| anyhow!("store {name}: unknown checksum {checksum:?} (sha256 or crc64nvme)"))?;
            builder = builder.with_checksum_algorithm(algorithm);
        }
        for (key, value) in &cfg.options {
            let key = AmazonS3ConfigKey::from_str(key).map_err(|e| anyhow!("store {name}: option {key}: {e}"))?;
            builder = builder.with_config(key, value);
        }
        let store = builder.build().with_context(|| format!("store {name}"))?;
        let base = normalize_prefix(prefix)?;
        let uri = if base.is_empty() {
            format!("s3://{bucket}")
        } else {
            format!("s3://{bucket}/{base}")
        };
        (Arc::new(store), base, uri)
    } else {
        let dir = PathBuf::from(cfg.url.strip_prefix("file://").unwrap_or(&cfg.url));
        std::fs::create_dir_all(&dir).with_context(|| format!("store {name}: cannot create {}", dir.display()))?;
        let dir = dir.canonicalize()?;
        let store = LocalFileSystem::new_with_prefix(&dir)?;
        let uri = url::Url::from_directory_path(&dir)
            .map(|u| u.as_str().trim_end_matches('/').to_string())
            .map_err(|_| anyhow!("store {name}: {} is not an absolute path", dir.display()))?;
        (Arc::new(store), String::new(), uri)
    };
    Ok(Arc::new(Store {
        name: name.to_string(),
        inner,
        base,
        uri,
        limiter,
    }))
}

pub fn open_stores(stores: &BTreeMap<String, StoreConfig>) -> Result<BTreeMap<String, Arc<Store>>> {
    stores
        .iter()
        .map(|(name, cfg)| Ok((name.clone(), open_store(name, cfg)?)))
        .collect()
}

struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: Option<String>,
}

fn ini_sections(text: &str) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut section = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            section = name.trim().to_string();
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            out.entry(section.clone())
                .or_default()
                .insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }
    out
}

/// Static keys for an AWS shared-config profile (`~/.aws/credentials`, region
/// from `~/.aws/config`). Role, SSO and process profiles need environment
/// credentials instead.
fn aws_profile(profile: &str) -> Result<Credentials> {
    let home = dirs::home_dir().context("no home directory")?;
    let creds_path = std::env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".aws").join("credentials"));
    let config_path = std::env::var_os("AWS_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".aws").join("config"));
    let creds = ini_sections(&std::fs::read_to_string(&creds_path).unwrap_or_default());
    let config = ini_sections(&std::fs::read_to_string(&config_path).unwrap_or_default());
    let config_section = if profile == "default" {
        config.get("default")
    } else {
        config.get(&format!("profile {profile}"))
    };
    let section = creds.get(profile).or(config_section).with_context(|| {
        format!(
            "no [{profile}] in {} or {}",
            creds_path.display(),
            config_path.display()
        )
    })?;
    let key = |k: &str| section.get(k).cloned();
    Ok(Credentials {
        access_key_id: key("aws_access_key_id").context("aws_access_key_id missing")?,
        secret_access_key: key("aws_secret_access_key").context("aws_secret_access_key missing")?,
        session_token: key("aws_session_token"),
        region: config_section.and_then(|s| s.get("region").cloned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_are_normalized_and_cannot_escape() {
        assert_eq!(normalize_prefix("/a//b\\c/./d/").unwrap(), "a/b/c/d");
        assert!(normalize_prefix("a/../b").is_err());
    }

    #[test]
    fn names_encode_reversibly_into_keys() {
        assert_eq!(encode_rel("u/a b.tif").unwrap(), "u/a b.tif");
        // A backslash is part of a name, not a separator.
        assert_eq!(encode_rel("u/a\\b.tif").unwrap(), "u/a%5Cb.tif");
        assert_ne!(encode_rel("u/a\\b.tif").unwrap(), encode_rel("u/a/b.tif").unwrap());
        assert_eq!(encode_rel("u/100%.tif").unwrap(), "u/100%25.tif");
        assert_eq!(encode_rel("u/a%5Cb.tif").unwrap(), "u/a%255Cb.tif");
        assert_eq!(encode_rel("u/line\nbreak").unwrap(), "u/line%0Abreak");
        assert_eq!(encode_rel("u/plaque_é.tif").unwrap(), "u/plaque_é.tif");
        assert!(encode_rel("u/../x").is_err());
        assert!(encode_rel("./x").is_err());
    }

    #[test]
    fn locations_join_prefixes_and_uris() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig {
            url: dir.path().to_string_lossy().into_owned(),
            endpoint: None,
            region: None,
            virtual_hosted_style: false,
            profile: None,
            allow_http: false,
            checksum: None,
            bandwidth: None,
            options: BTreeMap::new(),
        };
        let root = Location::root(open_store("local", &cfg).unwrap());
        let raw = root.child("workcell/raw").unwrap();
        assert_eq!(raw.key("u__1/a b.tif").unwrap().as_ref(), "workcell/raw/u__1/a b.tif");
        assert!(raw.uri_for("u__1").ends_with("/workcell/raw/u__1"));
        assert!(raw.uri_for("u__1").starts_with("file:///"));
    }

    #[test]
    fn s3_urls_split_bucket_and_prefix() {
        let cfg = StoreConfig {
            url: "s3://bucket/raw/data".into(),
            endpoint: Some("http://127.0.0.1:9".into()),
            region: Some("us-east-1".into()),
            virtual_hosted_style: false,
            profile: None,
            allow_http: true,
            checksum: Some("sha256".into()),
            bandwidth: None,
            options: BTreeMap::from([
                ("aws_access_key_id".into(), "k".into()),
                ("aws_secret_access_key".into(), "s".into()),
            ]),
        };
        let store = open_store("ceph", &cfg).unwrap();
        assert_eq!(
            (store.base.as_str(), store.uri.as_str()),
            ("raw/data", "s3://bucket/raw/data")
        );
        assert_eq!(Location::root(store).key("u/x").unwrap().as_ref(), "raw/data/u/x");
    }

    #[test]
    fn profiles_read_static_keys() {
        let dir = tempfile::tempdir().unwrap();
        let creds = dir.path().join("credentials");
        let config = dir.path().join("config");
        std::fs::write(&creds, "[lab]\naws_access_key_id = AK\naws_secret_access_key = SK\n").unwrap();
        std::fs::write(&config, "[profile lab]\nregion = us-west-2\n").unwrap();
        // SAFETY: tests touching these variables do not run concurrently with other readers.
        unsafe {
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &creds);
            std::env::set_var("AWS_CONFIG_FILE", &config);
        }
        let found = aws_profile("lab").unwrap();
        unsafe {
            std::env::remove_var("AWS_SHARED_CREDENTIALS_FILE");
            std::env::remove_var("AWS_CONFIG_FILE");
        }
        assert_eq!(
            (found.access_key_id.as_str(), found.secret_access_key.as_str()),
            ("AK", "SK")
        );
        assert_eq!(found.region.as_deref(), Some("us-west-2"));
    }
}
