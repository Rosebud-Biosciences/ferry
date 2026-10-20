//! Moving bytes: one read of a file, fanned out to every destination that needs
//! it, and verified moves to a backup drive.
//!
//! A file is read once. Each chunk becomes a [`Bytes`] -- reference-counted, so
//! handing it to a second destination copies nothing -- that goes to every
//! destination's single PUT (files up to `single_put_max`) or `WriteMultipart`
//! (bigger ones). The chunk is hashed (SHA-256) on the way. A destination that
//! fails mid-file drops out of the shared read and the others carry on; it is
//! retried later with a read of its own. With a shared read the slowest
//! destination paces the others, since a chunk is released only when every
//! destination has taken it.

use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use aws_lc_rs::digest;
use bytes::Bytes;
use chrono::Timelike;
use futures::StreamExt;
use futures::future::join_all;
use object_store::{ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::sync::Mutex;

use crate::config::{Bandwidth, Verify};
use crate::stores::Location;
use crate::util::mtime_ns;

#[derive(Debug, Clone, Copy)]
pub struct UploadSettings {
    pub single_put_max: u64,
    pub part_size: usize,
    pub part_concurrency: usize,
}

impl UploadSettings {
    pub fn from_limits(limits: &crate::config::LimitsConfig) -> Self {
        Self {
            single_put_max: limits.single_put_max,
            part_size: limits.part_size as usize,
            part_concurrency: limits.part_concurrency,
        }
    }
}

/// One destination of an upload.
#[derive(Debug, Clone)]
pub struct Target {
    pub dest: String,
    pub location: Location,
    pub rel: String,
    pub verify: Verify,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Copied {
    pub etag: Option<String>,
    pub verified: Verify,
}

#[derive(Debug)]
pub struct TargetResult {
    pub dest: String,
    pub uri: String,
    pub result: Result<Copied, String>,
}

#[derive(Debug)]
pub struct Fanout {
    pub size: u64,
    pub mtime_ns: i64,
    pub sha256: String,
    pub duration_ms: u64,
    pub multipart: bool,
    pub targets: Vec<TargetResult>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The file changed while it was read, so what was sent is not what is there.
    #[error("{0}")]
    Changed(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

fn stats(meta: &fs::Metadata) -> (u64, i64) {
    (meta.len(), mtime_ns(meta))
}

fn describe(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn sha_hex(d: digest::Digest) -> String {
    hex::encode(d.as_ref())
}

/// Upload `path` to every target, reading it once.
pub async fn upload(
    path: &Path,
    expect: Option<(u64, i64)>,
    targets: &[Target],
    settings: &UploadSettings,
) -> Result<Fanout, ReadError> {
    let before = tokio::fs::metadata(path).await?;
    let (size, mtime) = stats(&before);
    if expect.is_some_and(|e| e != (size, mtime)) {
        return Err(ReadError::Changed(format!(
            "{} changed since it was scanned",
            path.display()
        )));
    }
    let started = Instant::now();
    let multipart = size > settings.single_put_max;
    let (sha256, mut results) = if multipart {
        upload_multipart(path, size, targets, settings).await?
    } else {
        upload_single(path, targets).await?
    };
    let after = tokio::fs::metadata(path).await?;
    if stats(&after) != (size, mtime) {
        return Err(ReadError::Changed(format!(
            "{} changed while it was uploading",
            path.display()
        )));
    }
    let sha = sha256.as_str();
    let checks = join_all(results.iter().zip(targets).map(|(result, target)| async move {
        match result {
            Ok(copied) => verify(target, size, sha).await.map(|verified| Copied {
                etag: copied.etag.clone(),
                verified,
            }),
            Err(e) => Err(e.clone()),
        }
    }))
    .await;
    results = checks;
    Ok(Fanout {
        size,
        mtime_ns: mtime,
        duration_ms: started.elapsed().as_millis() as u64,
        multipart,
        targets: targets
            .iter()
            .zip(results)
            .map(|(t, result)| TargetResult {
                dest: t.dest.clone(),
                uri: t.location.uri_for(&t.rel),
                result,
            })
            .collect(),
        sha256,
    })
}

async fn upload_single(path: &Path, targets: &[Target]) -> Result<(String, Vec<Result<Copied, String>>), ReadError> {
    let owned = path.to_path_buf();
    let (data, sha256) = tokio::task::spawn_blocking(move || -> io::Result<(Bytes, String)> {
        let data = fs::read(&owned)?;
        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(&data);
        Ok((Bytes::from(data), sha_hex(ctx.finish())))
    })
    .await
    .map_err(|e| io::Error::other(e.to_string()))??;
    let results = join_all(targets.iter().map(|target| {
        let data = data.clone();
        async move {
            let key = target.location.key(&target.rel).map_err(describe)?;
            if let Some(limiter) = &target.location.store.limiter {
                limiter.acquire(data.len() as u64).await;
            }
            let put = target
                .location
                .store
                .inner
                .put(&key, PutPayload::from(data))
                .await
                .map_err(describe)?;
            Ok(Copied {
                etag: put.e_tag,
                verified: target.verify,
            })
        }
    }))
    .await;
    Ok((sha256, results))
}

async fn upload_multipart(
    path: &Path,
    size: u64,
    targets: &[Target],
    settings: &UploadSettings,
) -> Result<(String, Vec<Result<Copied, String>>), ReadError> {
    let mut results: Vec<Option<Result<Copied, String>>> = vec![None; targets.len()];
    let mut writers: Vec<Option<WriteMultipart>> = Vec::with_capacity(targets.len());
    for (i, target) in targets.iter().enumerate() {
        let started = async {
            let key = target.location.key(&target.rel).map_err(describe)?;
            target.location.store.inner.put_multipart(&key).await.map_err(describe)
        }
        .await;
        match started {
            Ok(upload) => writers.push(Some(WriteMultipart::new_with_chunk_size(upload, settings.part_size))),
            Err(e) => {
                results[i] = Some(Err(e));
                writers.push(None);
            }
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(2);
    let owned = path.to_path_buf();
    let part_size = settings.part_size;
    let reader = tokio::task::spawn_blocking(move || -> io::Result<String> {
        let mut file = fs::File::open(&owned)?;
        let mut ctx = digest::Context::new(&digest::SHA256);
        loop {
            let mut buf = vec![0u8; part_size];
            let mut filled = 0;
            while filled < part_size {
                match file.read(&mut buf[filled..])? {
                    0 => break,
                    n => filled += n,
                }
            }
            if filled == 0 {
                break;
            }
            buf.truncate(filled);
            ctx.update(&buf);
            if tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
                break; // every destination dropped out
            }
            if filled < part_size {
                break;
            }
        }
        Ok(sha_hex(ctx.finish()))
    });
    let mut sent: u64 = 0;
    let mut read_error: Option<io::Error> = None;
    while let Some(chunk) = rx.recv().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => {
                read_error = Some(e);
                break;
            }
        };
        sent += chunk.len() as u64;
        for (i, slot) in writers.iter_mut().enumerate() {
            let Some(writer) = slot else { continue };
            if let Some(limiter) = &targets[i].location.store.limiter {
                limiter.acquire(chunk.len() as u64).await;
            }
            if let Err(e) = writer.wait_for_capacity(settings.part_concurrency).await {
                results[i] = Some(Err(describe(e)));
                if let Some(writer) = slot.take() {
                    let _ = writer.abort().await;
                }
                continue;
            }
            writer.put(chunk.clone());
        }
        if writers.iter().all(Option::is_none) {
            break;
        }
    }
    drop(rx);
    let sha256 = match reader.await.map_err(|e| io::Error::other(e.to_string()))? {
        Ok(sha) => sha,
        Err(e) => {
            read_error.get_or_insert(e);
            String::new()
        }
    };
    let all_dropped = writers.iter().all(Option::is_none);
    if let Some(e) = read_error {
        abort_all(&mut writers).await;
        return Err(ReadError::Io(e));
    }
    if !all_dropped && sent != size {
        abort_all(&mut writers).await;
        return Err(ReadError::Changed(format!(
            "{} changed while it was uploading",
            path.display()
        )));
    }
    // A file that changed while it was read must not be committed anywhere.
    if let Ok(meta) = tokio::fs::metadata(path).await
        && meta.len() != size
    {
        abort_all(&mut writers).await;
        return Err(ReadError::Changed(format!(
            "{} changed while it was uploading",
            path.display()
        )));
    }
    let finishing = writers.into_iter().enumerate().filter_map(|(i, w)| w.map(|w| (i, w)));
    for (i, result) in join_all(finishing.map(|(i, w)| async move { (i, w.finish().await) })).await {
        results[i] = Some(
            result
                .map(|put| Copied {
                    etag: put.e_tag,
                    verified: targets[i].verify,
                })
                .map_err(describe),
        );
    }
    Ok((
        sha256,
        results
            .into_iter()
            .map(|r| r.unwrap_or_else(|| Err("upload did not run".into())))
            .collect(),
    ))
}

async fn abort_all(writers: &mut [Option<WriteMultipart>]) {
    for slot in writers.iter_mut() {
        if let Some(writer) = slot.take() {
            let _ = writer.abort().await;
        }
    }
}

/// Check a stored copy: its size always, its SHA-256 if asked to.
async fn verify(target: &Target, size: u64, sha256: &str) -> Result<Verify, String> {
    let key = target.location.key(&target.rel).map_err(describe)?;
    let store = &target.location.store.inner;
    let meta = store.head(&key).await.map_err(describe)?;
    if meta.size != size {
        return Err(format!(
            "{} holds {} bytes; sent {size}",
            target.location.uri_for(&target.rel),
            meta.size
        ));
    }
    if target.verify == Verify::Sha256 {
        let got = readback_sha256(target).await?;
        if got != sha256 {
            return Err(format!(
                "{} reads back with SHA-256 {got}; sent {sha256}",
                target.location.uri_for(&target.rel)
            ));
        }
    }
    Ok(target.verify)
}

/// A local file's SHA-256, and its size and mtime before and after the read.
pub struct Hashed {
    pub sha256: String,
    pub before: (u64, i64),
    pub after: (u64, i64),
}

impl Hashed {
    /// The file had size and mtime `stats` throughout the read.
    pub fn steady_at(&self, stats: (u64, i64)) -> bool {
        self.before == stats && self.after == stats
    }
}

pub fn hash_file(path: &Path) -> io::Result<Hashed> {
    let before = stats(&fs::metadata(path)?);
    let mut file = fs::File::open(path)?;
    let mut ctx = digest::Context::new(&digest::SHA256);
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
    }
    let after = stats(&fs::metadata(path)?);
    Ok(Hashed {
        sha256: sha_hex(ctx.finish()),
        before,
        after,
    })
}

pub async fn readback_sha256(target: &Target) -> Result<String, String> {
    let key = target.location.key(&target.rel).map_err(describe)?;
    let mut stream = target
        .location
        .store
        .inner
        .get(&key)
        .await
        .map_err(describe)?
        .into_stream();
    let mut ctx = digest::Context::new(&digest::SHA256);
    while let Some(chunk) = stream.next().await {
        ctx.update(&chunk.map_err(describe)?);
    }
    Ok(sha_hex(ctx.finish()))
}

// -- bandwidth ----------------------------------------------------------------

/// A token bucket over bytes per second, optionally only in local time windows.
/// Shared by every upload to a store, so the cap is on the store's total rate.
/// A chunk bigger than one second's allowance is let through and paid for by
/// waiting afterwards, so a part never has to be split to fit.
pub struct RateLimiter {
    windows: Vec<(u16, u16, f64)>,
    bucket: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    pub fn new(bandwidth: &Bandwidth) -> Self {
        let windows = match bandwidth {
            Bandwidth::Always(rate) => vec![(0, 24 * 60, *rate as f64)],
            Bandwidth::Windows(list) => list.iter().map(|&(a, b, r)| (a, b, r as f64)).collect(),
        };
        Self {
            windows,
            bucket: Mutex::new((0.0, Instant::now())),
        }
    }

    pub fn rate_at(&self, minute: u16) -> Option<f64> {
        self.windows.iter().find_map(|&(start, end, rate)| {
            let inside = if start <= end {
                start <= minute && minute < end
            } else {
                minute >= start || minute < end
            };
            inside.then_some(rate)
        })
    }

    pub async fn acquire(&self, nbytes: u64) {
        let now = chrono::Local::now();
        let Some(rate) = self
            .rate_at((now.hour() * 60 + now.minute()) as u16)
            .filter(|r| *r > 0.0)
        else {
            return;
        };
        let mut bucket = self.bucket.lock().await;
        let (tokens, wait) = debit(bucket.0, bucket.1.elapsed().as_secs_f64(), rate, nbytes);
        if wait > 0.0 {
            tokio::time::sleep(std::time::Duration::from_secs_f64(wait)).await;
        }
        *bucket = (tokens, Instant::now());
    }
}

/// Refill `tokens` for `elapsed` seconds at `rate` (capped at one second's
/// worth), take `nbytes`; return the new balance and how long to wait.
pub fn debit(tokens: f64, elapsed: f64, rate: f64, nbytes: u64) -> (f64, f64) {
    let balance = (tokens + elapsed * rate).min(rate) - nbytes as f64;
    if balance < 0.0 {
        (0.0, -balance / rate)
    } else {
        (balance, 0.0)
    }
}

// -- local moves --------------------------------------------------------------

/// Move `src` to `dst` and return its size. Within one file system this is a
/// rename. Across file systems the copy is written under a temporary name,
/// fsync'd and size-checked, then renamed into place and `src` deleted, so
/// neither a crash nor a short copy leaves the only good copy half-written.
pub fn move_file(src: &Path, dst: &Path, sha256: Option<&str>) -> io::Result<u64> {
    let meta = fs::metadata(src)?;
    let size = meta.len();
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::rename(src, dst) {
        Ok(()) => return Ok(size),
        Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {}
        Err(e) => return Err(e),
    }
    copy_across(src, dst, &meta, sha256)
}

/// Copy `src` to `dst` on another file system, hashing on the way: the copy
/// must have the size and (if given) the SHA-256 of what was uploaded before
/// `src` is removed.
pub fn copy_across(src: &Path, dst: &Path, meta: &fs::Metadata, sha256: Option<&str>) -> io::Result<u64> {
    let size = meta.len();
    let mut partial = dst.as_os_str().to_owned();
    partial.push(".ferry-partial");
    let partial = std::path::PathBuf::from(partial);
    let mut reader = fs::File::open(src)?;
    let mut writer = fs::File::create(&partial)?;
    let mut ctx = digest::Context::new(&digest::SHA256);
    let mut buf = vec![0u8; 1 << 20];
    let mut copied = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
        io::Write::write_all(&mut writer, &buf[..n])?;
        copied += n as u64;
    }
    if let Ok(modified) = meta.modified() {
        let _ = writer.set_modified(modified);
    }
    writer.sync_all()?;
    drop(writer);
    let got = sha_hex(ctx.finish());
    let wrong = if copied != size {
        Some(format!("copy of {} is {copied} bytes, expected {size}", src.display()))
    } else {
        sha256
            .filter(|want| *want != got)
            .map(|want| format!("{} hashes to {got}, not {want} as uploaded", src.display()))
    };
    if let Some(error) = wrong {
        let _ = fs::remove_file(&partial);
        return Err(io::Error::other(error));
    }
    fs::rename(&partial, dst)?;
    fs::remove_file(src)?;
    Ok(size)
}

/// Remove empty directories under (and including) `top`; never deletes data.
pub fn prune_empty_dirs(top: &Path) {
    fn walk(dir: &Path) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|t| t.is_dir()) {
                    walk(&entry.path());
                }
            }
        }
        let _ = fs::remove_dir(dir);
    }
    walk(top);
}

pub type SharedLimiter = Arc<RateLimiter>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use crate::stores::open_store;
    use std::collections::BTreeMap;

    fn local(dir: &Path) -> Location {
        let cfg = StoreConfig {
            url: dir.to_string_lossy().into_owned(),
            endpoint: None,
            region: None,
            virtual_hosted_style: false,
            profile: None,
            allow_http: false,
            checksum: None,
            bandwidth: None,
            options: BTreeMap::new(),
        };
        Location::root(open_store("t", &cfg).unwrap())
    }

    fn sha(data: &[u8]) -> String {
        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(data);
        sha_hex(ctx.finish())
    }

    fn targets(dirs: &[&Path], verify: Verify) -> Vec<Target> {
        dirs.iter()
            .enumerate()
            .map(|(i, d)| Target {
                dest: format!("d{i}"),
                location: local(d),
                rel: "u/x.bin".into(),
                verify,
            })
            .collect()
    }

    const SMALL: UploadSettings = UploadSettings {
        single_put_max: 1024,
        part_size: 64 * 1024,
        part_concurrency: 2,
    };

    #[tokio::test]
    async fn one_read_reaches_every_destination() {
        let dir = tempfile::tempdir().unwrap();
        for (len, multipart) in [(100usize, false), (300 * 1024, true)] {
            let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let src = dir.path().join(format!("src-{len}.bin"));
            fs::write(&src, &body).unwrap();
            let (a, b) = (dir.path().join(format!("a{len}")), dir.path().join(format!("b{len}")));
            let out = upload(&src, None, &targets(&[&a, &b], Verify::Sha256), &SMALL)
                .await
                .unwrap();
            assert_eq!(out.multipart, multipart);
            assert_eq!(out.sha256, sha(&body));
            assert!(out.targets.iter().all(|t| t.result
                == Ok(Copied {
                    etag: t.result.as_ref().unwrap().etag.clone(),
                    verified: Verify::Sha256
                })));
            assert_eq!(fs::read(a.join("u/x.bin")).unwrap(), body);
            assert_eq!(fs::read(b.join("u/x.bin")).unwrap(), body);
        }
    }

    #[tokio::test]
    async fn a_stale_scan_is_refused_before_anything_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("x.bin");
        fs::write(&src, b"now").unwrap();
        let out = upload(
            &src,
            Some((1, 1)),
            &targets(&[&dir.path().join("a")], Verify::Size),
            &SMALL,
        )
        .await;
        assert!(matches!(out, Err(ReadError::Changed(m)) if m.contains("since it was scanned")));
        assert!(!dir.path().join("a/u/x.bin").exists());
    }

    type Tamper = Arc<std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>>;

    fn fire(tamper: &Tamper) {
        if let Some(tamper) = tamper.lock().unwrap().take() {
            tamper();
        }
    }

    /// A local store that runs `tamper` once, as the first write lands (a
    /// single PUT, or a multipart upload's first part).
    struct Tampering {
        inner: Arc<dyn object_store::ObjectStore>,
        tamper: Tamper,
    }

    impl std::fmt::Debug for Tampering {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Tampering({})", self.inner)
        }
    }

    impl std::fmt::Display for Tampering {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Tampering({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for Tampering {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            fire(&self.tamper);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(TamperingUpload {
                inner,
                tamper: self.tamper.clone(),
            }))
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct TamperingUpload {
        inner: Box<dyn object_store::MultipartUpload>,
        tamper: Tamper,
    }

    impl std::fmt::Debug for TamperingUpload {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TamperingUpload")
        }
    }

    #[async_trait::async_trait]
    impl object_store::MultipartUpload for TamperingUpload {
        fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
            fire(&self.tamper);
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }

    fn tampering(dir: &Path, tamper: impl FnOnce() + Send + 'static) -> Vec<Target> {
        let plain = local(dir);
        let store = Arc::new(crate::stores::Store {
            name: plain.store.name.clone(),
            inner: Arc::new(Tampering {
                inner: plain.store.inner.clone(),
                tamper: Arc::new(std::sync::Mutex::new(Some(Box::new(tamper)))),
            }),
            base: plain.store.base.clone(),
            uri: plain.store.uri.clone(),
            limiter: None,
        });
        vec![Target {
            dest: "d0".into(),
            location: Location::root(store),
            rel: "u/x.bin".into(),
            verify: Verify::Size,
        }]
    }

    #[tokio::test]
    async fn a_file_rewritten_during_a_single_put_is_not_taken_as_sent() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("x.bin");
        fs::write(&src, b"first").unwrap();
        let modified = fs::metadata(&src).unwrap().modified().unwrap();
        let rewrite = src.clone();
        let targets = tampering(&dir.path().join("a"), move || {
            fs::write(&rewrite, b"FIRST").unwrap();
            let file = fs::File::options().write(true).open(&rewrite).unwrap();
            file.set_modified(modified + std::time::Duration::from_secs(2)).unwrap();
        });

        let out = upload(&src, None, &targets, &SMALL).await;
        assert!(
            matches!(&out, Err(ReadError::Changed(m)) if m.contains("while it was uploading")),
            "{out:?}"
        );
    }

    // Replacing the file leaves the reader on the old one, which it reads to
    // the end: only the length check before committing can tell.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_replaced_during_a_multipart_upload_is_never_committed() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("x.bin");
        fs::write(&src, vec![1u8; 100 * 1024]).unwrap();
        let (replace, staged) = (src.clone(), dir.path().join("x.new"));
        let remote = dir.path().join("a");
        let targets = tampering(&remote, move || {
            fs::write(&staged, vec![2u8; 200 * 1024]).unwrap();
            fs::rename(&staged, &replace).unwrap();
        });

        let out = upload(&src, None, &targets, &SMALL).await;
        assert!(
            matches!(&out, Err(ReadError::Changed(m)) if m.contains("while it was uploading")),
            "{out:?}"
        );
        assert!(!remote.join("u/x.bin").exists(), "nothing was committed");
    }

    #[tokio::test]
    async fn verification_catches_a_short_or_corrupted_copy() {
        let dir = tempfile::tempdir().unwrap();
        let remote = dir.path().join("remote");
        fs::create_dir_all(remote.join("u")).unwrap();
        fs::write(remote.join("u/x.bin"), b"abcd").unwrap();
        let by_size = &targets(&[&remote], Verify::Size)[0];
        let by_hash = &targets(&[&remote], Verify::Sha256)[0];

        assert_eq!(verify(by_size, 4, &sha(b"abcd")).await, Ok(Verify::Size));
        assert_eq!(verify(by_hash, 4, &sha(b"abcd")).await, Ok(Verify::Sha256));
        let short = verify(by_size, 5, &sha(b"abcde")).await.unwrap_err();
        assert!(short.contains("holds 4 bytes; sent 5"), "{short}");
        // Same size, different bytes: only a readback notices.
        assert_eq!(verify(by_size, 4, &sha(b"wxyz")).await, Ok(Verify::Size));
        let corrupt = verify(by_hash, 4, &sha(b"wxyz")).await.unwrap_err();
        assert!(corrupt.contains("reads back with SHA-256"), "{corrupt}");
    }

    #[test]
    fn the_bucket_pays_for_big_chunks_by_waiting() {
        assert_eq!(debit(0.0, 0.0, 100.0, 100), (0.0, 1.0));
        assert_eq!(debit(0.0, 10.0, 100.0, 100), (0.0, 0.0)); // refilled, capped at one second
        assert_eq!(debit(0.0, 10.0, 100.0, 50), (50.0, 0.0));
        assert_eq!(debit(0.0, 0.5, 100.0, 150), (0.0, 1.0));
    }

    #[test]
    fn rate_windows_apply_only_inside_them() {
        let limiter = RateLimiter::new(&Bandwidth::Windows(vec![(8 * 60, 18 * 60, 10), (22 * 60, 6 * 60, 5)]));
        assert_eq!(limiter.rate_at(9 * 60), Some(10.0));
        assert_eq!(limiter.rate_at(23 * 60), Some(5.0));
        assert_eq!(limiter.rate_at(3 * 60), Some(5.0));
        assert_eq!(limiter.rate_at(19 * 60), None);
        assert_eq!(limiter.rate_at(6 * 60), None);
    }

    #[test]
    fn moves_rename_within_a_file_system_and_verify_across() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("a/x.bin");
        fs::create_dir_all(src.parent().unwrap()).unwrap();
        fs::write(&src, b"payload").unwrap();
        let dst = dir.path().join("b/deep/x.bin");
        assert_eq!(move_file(&src, &dst, None).unwrap(), 7);
        assert_eq!(fs::read(&dst).unwrap(), b"payload");
        assert!(!src.exists());

        fs::write(&src, b"payload").unwrap();
        let meta = fs::metadata(&src).unwrap();
        let dst = dir.path().join("c/x.bin");
        fs::create_dir_all(dst.parent().unwrap()).unwrap();
        assert_eq!(copy_across(&src, &dst, &meta, Some(&sha(b"payload"))).unwrap(), 7);
        assert_eq!(fs::read(&dst).unwrap(), b"payload");
        assert_eq!(
            fs::metadata(&dst).unwrap().modified().unwrap(),
            meta.modified().unwrap()
        );
        assert!(!src.exists());
        assert_eq!(fs::read_dir(dst.parent().unwrap()).unwrap().count(), 1);
    }

    #[test]
    fn a_copy_that_does_not_hash_as_uploaded_leaves_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("x.bin");
        fs::write(&src, b"rewritten").unwrap();
        let meta = fs::metadata(&src).unwrap();
        let dst = dir.path().join("backup.bin");

        let err = copy_across(&src, &dst, &meta, Some(&sha(b"original!"))).unwrap_err();
        assert!(err.to_string().contains("not"), "{err}");
        assert!(src.exists() && !dst.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no partial copy left");
    }

    #[test]
    fn pruning_removes_only_empty_directories() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("u/a/b")).unwrap();
        fs::create_dir_all(dir.path().join("u/keep")).unwrap();
        fs::write(dir.path().join("u/keep/f.txt"), b"x").unwrap();
        prune_empty_dirs(&dir.path().join("u"));
        assert!(!dir.path().join("u/a").exists());
        assert!(dir.path().join("u/keep/f.txt").exists());
    }
}
