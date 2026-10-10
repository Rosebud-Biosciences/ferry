//! Units: how a pipeline's files group into acquisitions, and scanning for them.
//!
//! A unit is a directory at a fixed depth (`unit.dirs`, one glob per level) or
//! the set of files whose root-relative path gives the same `unit` group of
//! `unit.regex`. A file's *within* path is relative to its unit (for regex units,
//! its path from the root) and it travels to `<prefix>/<unit>/<within>` in every
//! destination, so a directory unit keeps the local layout exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use regex::Regex;

use crate::config::PipelineConfig;
use crate::globs::{GlobList, PathGlob};
use crate::util::mtime_ns;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileObs {
    pub rel: String,
    pub within: String,
    pub size: u64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, Default)]
pub struct UnitObs {
    pub key: String,
    pub files: BTreeMap<String, FileObs>,
    pub marker: bool,
    /// The newest mtime of anything in the unit that is not excluded -- files
    /// ferry does not upload and the directories themselves included, since a
    /// directory's mtime moves whenever an entry is added, removed or renamed.
    pub newest_ns: i64,
    /// What could not be read in the unit. While anything is unread, a file
    /// ferry did not see may still be there: none counts as gone, and the
    /// unit neither completes, registers nor finalizes.
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Scan {
    pub units: BTreeMap<String, UnitObs>,
    /// Unit keys this scan looked at in full (`None`: the whole tree), so the
    /// journal can tell "deleted" from "not looked at".
    pub scope: Option<BTreeSet<String>>,
    /// What could not be read outside any unit. A unit there may exist
    /// unseen, so no unit counts as gone.
    pub errors: Vec<String>,
    /// The errors could hide any unit's files (a regex layout gathers files
    /// from anywhere): every unit counts as not fully read.
    pub unattributed: bool,
}

pub struct Layout {
    pub root: PathBuf,
    include: GlobList,
    exclude: GlobList,
    marker: Option<PathGlob>,
    regex: Option<Regex>,
    levels: Vec<PathGlob>,
}

struct Entry {
    name: String,
    path: PathBuf,
    kind: Kind,
    size: u64,
    mtime_ns: i64,
}

#[derive(PartialEq)]
enum Kind {
    Dir,
    File,
    /// A symbolic link (or junction), never followed: its target may be
    /// anywhere, and ferry ships only what is under the root.
    Link,
}

/// Appended to a file's name while it is being finalized: it is set aside by
/// a rename first, so a writer that comes back makes a new file instead of
/// changing the bytes being deleted or moved. Scans never see these.
pub const HELD_SUFFIX: &str = ".ferry-finalizing";

/// Where `path` waits while it is finalized.
pub fn held_path(path: &Path) -> PathBuf {
    let mut held = path.as_os_str().to_owned();
    held.push(HELD_SUFFIX);
    PathBuf::from(held)
}

fn link_error(path: &Path) -> String {
    format!(
        "{}: symbolic links are not followed; exclude it, or replace it with what it points to",
        path.display()
    )
}

impl Layout {
    pub fn new(cfg: &PipelineConfig) -> Result<Self> {
        let regex = cfg.unit.regex.as_deref().map(Regex::new).transpose()?;
        let levels = if regex.is_some() {
            Vec::new()
        } else {
            cfg.unit
                .levels()
                .iter()
                .map(|level| PathGlob::new(level))
                .collect::<Result<_>>()?
        };
        Ok(Self {
            root: cfg.root.clone(),
            include: GlobList::new(&cfg.include)?,
            exclude: GlobList::new(&cfg.exclude)?,
            marker: cfg.marker.as_deref().map(PathGlob::new).transpose()?,
            regex,
            levels,
        })
    }

    pub fn by_dirs(&self) -> bool {
        self.regex.is_none()
    }

    pub fn unit_dir(&self, key: &str) -> Option<PathBuf> {
        self.by_dirs().then(|| self.root.join(key))
    }

    pub fn remote_rel(key: &str, within: &str) -> String {
        format!("{key}/{within}")
    }

    /// The unit a root-relative path belongs to, if it can be told from the path.
    pub fn key_for(&self, rel: &str) -> Option<String> {
        if let Some(re) = &self.regex {
            return re.captures(rel).and_then(|c| regex_key(&c));
        }
        let parts: Vec<&str> = rel.split('/').collect();
        if parts.len() < self.levels.len() {
            return None;
        }
        let head = &parts[..self.levels.len()];
        self.levels
            .iter()
            .zip(head)
            .all(|(glob, part)| glob.is_match(part))
            .then(|| head.join("/"))
    }

    /// A directory's entries; what cannot be read is described in `errors`.
    fn entries(path: &Path, errors: &mut Vec<String>) -> Vec<Entry> {
        let read = match fs::read_dir(path) {
            Ok(read) => read,
            Err(e) => {
                errors.push(format!("cannot list {}: {e}", path.display()));
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for entry in read {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    errors.push(format!("cannot list {}: {e}", path.display()));
                    continue;
                }
            };
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                errors.push(format!("{}: the name is not valid Unicode", path.display()));
                continue;
            };
            if name.ends_with(HELD_SUFFIX) {
                continue;
            }
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(e) => {
                    errors.push(format!("cannot read {}: {e}", path.display()));
                    continue;
                }
            };
            if file_type.is_symlink() {
                out.push(Entry {
                    name,
                    path,
                    kind: Kind::Link,
                    size: 0,
                    mtime_ns: 0,
                });
                continue;
            }
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                Err(e) => {
                    errors.push(format!("cannot read {}: {e}", path.display()));
                    continue;
                }
            };
            let kind = if meta.is_dir() {
                Kind::Dir
            } else if meta.is_file() {
                Kind::File
            } else {
                continue;
            };
            out.push(Entry {
                name,
                path,
                kind,
                size: meta.len(),
                mtime_ns: mtime_ns(&meta),
            });
        }
        out
    }

    /// Directories at unit depth, without walking into them (cheap discovery).
    pub fn unit_keys(&self) -> Vec<String> {
        if !self.by_dirs() {
            return Vec::new();
        }
        self.unit_dirs(&mut Vec::new())
    }

    fn unit_dirs(&self, errors: &mut Vec<String>) -> Vec<String> {
        let mut frontier = vec![(String::new(), self.root.clone())];
        for level in &self.levels {
            let mut deeper = Vec::new();
            for (prefix, path) in &frontier {
                for entry in Self::entries(path, errors) {
                    let rel = if prefix.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{prefix}/{}", entry.name)
                    };
                    if !level.is_match(&entry.name) || self.exclude.is_match(&rel) {
                        continue;
                    }
                    match entry.kind {
                        Kind::Dir => deeper.push((rel, entry.path)),
                        Kind::Link => errors.push(link_error(&entry.path)),
                        Kind::File => {}
                    }
                }
            }
            frontier = deeper;
        }
        let mut keys: Vec<String> = frontier.into_iter().map(|(rel, _)| rel).collect();
        keys.sort();
        keys
    }

    /// Observe the whole tree, or just the units in `keys` (directory units).
    pub fn scan(&self, keys: Option<&[String]>) -> Scan {
        let mut errors = Vec::new();
        if self.regex.is_some() {
            let units = self.scan_regex(&mut errors);
            return Scan {
                units,
                scope: None,
                unattributed: !errors.is_empty(),
                errors,
            };
        }
        match keys {
            None => {
                let units = self
                    .unit_dirs(&mut errors)
                    .into_iter()
                    .map(|key| {
                        let obs = self.scan_dir(&key);
                        (key, obs)
                    })
                    .collect();
                Scan {
                    units,
                    scope: None,
                    errors,
                    unattributed: false,
                }
            }
            Some(keys) => {
                let scope: BTreeSet<String> = keys.iter().cloned().collect();
                let mut units = BTreeMap::new();
                for key in &scope {
                    let top = self.root.join(key);
                    match fs::symlink_metadata(&top) {
                        Ok(meta) if meta.is_dir() => {
                            units.insert(key.clone(), self.scan_dir(key));
                        }
                        Ok(meta) if meta.file_type().is_symlink() => {
                            units.insert(
                                key.clone(),
                                UnitObs {
                                    key: key.clone(),
                                    errors: vec![link_error(&top)],
                                    ..Default::default()
                                },
                            );
                        }
                        // Gone, or no longer a directory.
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        // Cannot tell: its files stay as last seen.
                        Err(e) => {
                            units.insert(
                                key.clone(),
                                UnitObs {
                                    key: key.clone(),
                                    errors: vec![format!("cannot read {}: {e}", top.display())],
                                    ..Default::default()
                                },
                            );
                        }
                    }
                }
                Scan {
                    units,
                    scope: Some(scope),
                    errors,
                    unattributed: false,
                }
            }
        }
    }

    fn scan_dir(&self, key: &str) -> UnitObs {
        let mut obs = UnitObs {
            key: key.to_string(),
            ..Default::default()
        };
        let top = self.root.join(key);
        match fs::metadata(&top) {
            Ok(meta) => obs.newest_ns = mtime_ns(&meta),
            Err(e) => obs.errors.push(format!("cannot read {}: {e}", top.display())),
        }
        let mut stack = vec![(top, String::new())];
        while let Some((path, prefix)) = stack.pop() {
            for entry in Self::entries(&path, &mut obs.errors) {
                let within = if prefix.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                let rel = format!("{key}/{within}");
                if self.exclude.is_match(&rel) {
                    continue;
                }
                match entry.kind {
                    Kind::Link => {
                        obs.errors.push(link_error(&entry.path));
                        continue;
                    }
                    Kind::Dir => {
                        obs.newest_ns = obs.newest_ns.max(entry.mtime_ns);
                        stack.push((entry.path, within));
                        continue;
                    }
                    Kind::File => obs.newest_ns = obs.newest_ns.max(entry.mtime_ns),
                }
                if self.marker.as_ref().is_some_and(|m| m.is_match(&within)) {
                    obs.marker = true;
                }
                if self.include.is_match(&within) {
                    obs.files.insert(
                        rel.clone(),
                        FileObs {
                            rel,
                            within,
                            size: entry.size,
                            mtime_ns: entry.mtime_ns,
                        },
                    );
                }
            }
        }
        obs
    }

    fn scan_regex(&self, errors: &mut Vec<String>) -> BTreeMap<String, UnitObs> {
        let re = self.regex.as_ref().expect("regex layout");
        let mut units: BTreeMap<String, UnitObs> = BTreeMap::new();
        let mut stack = vec![(self.root.clone(), String::new())];
        while let Some((path, prefix)) = stack.pop() {
            for entry in Self::entries(&path, errors) {
                let rel = if prefix.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                if self.exclude.is_match(&rel) {
                    continue;
                }
                match entry.kind {
                    Kind::Link => {
                        errors.push(link_error(&entry.path));
                        continue;
                    }
                    Kind::Dir => {
                        stack.push((entry.path, rel));
                        continue;
                    }
                    Kind::File => {}
                }
                let Some(key) = re.captures(&rel).and_then(|c| regex_key(&c)) else {
                    continue;
                };
                let obs = units.entry(key.clone()).or_insert_with(|| UnitObs {
                    key,
                    ..Default::default()
                });
                obs.newest_ns = obs.newest_ns.max(entry.mtime_ns);
                if self.marker.as_ref().is_some_and(|m| m.is_match(&rel)) {
                    obs.marker = true;
                }
                if self.include.is_match(&rel) {
                    obs.files.insert(
                        rel.clone(),
                        FileObs {
                            within: rel.clone(),
                            rel,
                            size: entry.size,
                            mtime_ns: entry.mtime_ns,
                        },
                    );
                }
            }
        }
        units
    }
}

fn regex_key(caps: &regex::Captures) -> Option<String> {
    let raw = caps.name("unit")?.as_str();
    let key = raw
        .split('/')
        .filter(|p| !p.is_empty() && *p != "." && *p != "..")
        .collect::<Vec<_>>()
        .join("/");
    (!key.is_empty()).then_some(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::time::{Duration, SystemTime};

    fn write(path: &Path, body: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }

    fn set_mtime(path: &Path, secs_ago: u64) {
        let when = SystemTime::now() - Duration::from_secs(secs_ago);
        filetime::set_file_mtime(path, filetime::FileTime::from_system_time(when)).unwrap();
    }

    fn layout(root: &Path, extra: &str) -> Layout {
        let text = format!(
            "state_dir: {state}\nstores: {{s: {{url: 's3://b'}}}}\npipelines:\n  - name: inst\n    root: {root}\n    destinations: [s]\n{extra}",
            state = root.with_extension("state").display(),
            root = root.display(),
        );
        let cfg = Config::from_yaml(&text, root).unwrap();
        Layout::new(&cfg.pipelines[0]).unwrap()
    }

    #[test]
    fn directory_units_at_depth_two() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("plates/p1__a/x.tif"), b"xx");
        write(&root.join("plates/p1__a/deep/y.tif"), b"y");
        write(&root.join("plates/skip/z.tif"), b"");
        write(&root.join("other/p2__b/z.tif"), b"");
        write(&root.join("loose.tif"), b"");

        let scan = layout(root, "    unit: {dirs: 'plates/*__*'}\n").scan(None);

        assert_eq!(scan.units.keys().collect::<Vec<_>>(), ["plates/p1__a"]);
        let unit = &scan.units["plates/p1__a"];
        let files: Vec<(&str, &str, u64)> = unit
            .files
            .values()
            .map(|f| (f.rel.as_str(), f.within.as_str(), f.size))
            .collect();
        assert_eq!(
            files,
            [
                ("plates/p1__a/deep/y.tif", "deep/y.tif", 1),
                ("plates/p1__a/x.tif", "x.tif", 2),
            ]
        );
        assert!(scan.scope.is_none());
    }

    #[test]
    fn excluded_entries_are_invisible_even_to_activity() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("u/a.tif"), b"a");
        write(&root.join("u/a.tif.tmp"), b"busy");
        set_mtime(&root.join("u/a.tif"), 3600);
        set_mtime(&root.join("u"), 7200);

        let unit = layout(root, "    exclude: ['*.tmp']\n")
            .scan(None)
            .units
            .remove("u")
            .unwrap();

        assert_eq!(unit.files.keys().collect::<Vec<_>>(), ["u/a.tif"]);
        assert_eq!(unit.newest_ns, mtime_ns(&fs::metadata(root.join("u/a.tif")).unwrap()));
    }

    #[test]
    fn activity_counts_unshipped_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("u/a.tif"), b"a");
        write(&root.join("u/log.txt"), b"l");
        set_mtime(&root.join("u/a.tif"), 3600);
        set_mtime(&root.join("u/log.txt"), 5);
        set_mtime(&root.join("u"), 7200);

        let lay = layout(root, "    include: ['*.tif']\n");
        let unit = lay.scan(None).units.remove("u").unwrap();
        assert_eq!(unit.files.keys().collect::<Vec<_>>(), ["u/a.tif"]);
        assert_eq!(unit.newest_ns, mtime_ns(&fs::metadata(root.join("u/log.txt")).unwrap()));

        set_mtime(&root.join("u/log.txt"), 7200);
        fs::create_dir(root.join("u/new_dir")).unwrap();
        let unit = lay.scan(None).units.remove("u").unwrap();
        let newest = mtime_ns(&fs::metadata(root.join("u")).unwrap())
            .max(mtime_ns(&fs::metadata(root.join("u/new_dir")).unwrap()));
        assert_eq!(unit.newest_ns, newest);
    }

    #[test]
    fn markers_are_found_whether_or_not_they_are_shipped() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("u/images/index.xml"), b"");
        write(&root.join("v/images/a.tiff"), b"");

        let scan = layout(root, "    include: ['*.tiff']\n    marker: images/index.xml\n").scan(None);

        assert!(scan.units["u"].marker);
        assert!(scan.units["u"].files.is_empty());
        assert!(!scan.units["v"].marker);
    }

    #[test]
    fn scoped_scans_report_what_they_looked_at() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a/x.tif"), b"");
        write(&root.join("b/x.tif"), b"");

        let scan = layout(root, "").scan(Some(&["a".into(), "gone".into()]));

        assert_eq!(scan.units.keys().collect::<Vec<_>>(), ["a"]);
        assert_eq!(scan.scope.unwrap().into_iter().collect::<Vec<_>>(), ["a", "gone"]);
    }

    #[test]
    fn regex_units_gather_files_from_anywhere() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("RUN1_quant.csv"), b"");
        write(&root.join("2026/RUN1_melt.csv"), b"");
        write(&root.join("RUN2_quant.csv"), b"");
        write(&root.join("README"), b"");

        let lay = layout(root, "    unit: {regex: '(?:^|/)(?P<unit>RUN\\d+)_'}\n");
        let scan = lay.scan(None);

        assert_eq!(scan.units.keys().collect::<Vec<_>>(), ["RUN1", "RUN2"]);
        let within: BTreeSet<&str> = scan.units["RUN1"].files.values().map(|f| f.within.as_str()).collect();
        assert_eq!(within, BTreeSet::from(["2026/RUN1_melt.csv", "RUN1_quant.csv"]));
        assert_eq!(lay.key_for("RUN7_a.csv"), Some("RUN7".into()));
    }

    #[test]
    fn key_for_maps_event_paths_to_units() {
        let dir = tempfile::tempdir().unwrap();
        let lay = layout(dir.path(), "    unit: {dirs: '*/run*'}\n");
        assert_eq!(lay.key_for("plate1/run2/a/b.tif").as_deref(), Some("plate1/run2"));
        assert_eq!(lay.key_for("plate1/run2").as_deref(), Some("plate1/run2"));
        assert_eq!(lay.key_for("plate1/other/b.tif"), None);
        assert_eq!(lay.key_for("plate1"), None);
    }

    #[test]
    fn unit_keys_lists_unit_directories_without_walking_them() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a__1/x.tif"), b"");
        write(&root.join("b__2/deep/x.tif"), b"");
        write(&root.join("plain/x.tif"), b"");
        assert_eq!(layout(root, "    unit: {dirs: '*__*'}\n").unit_keys(), ["a__1", "b__2"]);
    }

    /// Make `path` unreadable; `false` if this process reads it anyway (root).
    #[cfg(unix)]
    fn lock_out(path: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o000)).unwrap();
        fs::read_dir(path).is_err()
    }

    #[cfg(unix)]
    fn let_in(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn what_cannot_be_read_is_reported_where_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        write(&root.join("u/a.tif"), b"a");
        write(&root.join("u/sub/b.tif"), b"b");
        write(&root.join("v/c.tif"), b"c");
        let lay = layout(&root, "");

        let locked = lock_out(&root.join("u/sub"));
        let scan = lay.scan(None);
        let_in(&root.join("u/sub"));
        if !locked {
            return;
        }
        assert!(scan.errors.is_empty());
        assert_eq!(scan.units["u"].files.keys().collect::<Vec<_>>(), ["u/a.tif"]);
        assert!(
            scan.units["u"].errors[0].contains("cannot list"),
            "{:?}",
            scan.units["u"].errors
        );
        assert!(scan.units["v"].errors.is_empty());

        lock_out(&root);
        let full = lay.scan(None);
        let scoped = lay.scan(Some(&["u".into()]));
        let_in(&root);
        assert!(full.units.is_empty());
        assert!(full.errors[0].contains("cannot list"), "{:?}", full.errors);
        assert!(
            !scoped.units["u"].errors.is_empty(),
            "a unit that cannot be looked at is not gone"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_reported_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        write(&dir.path().join("outside.tif"), b"not ours");
        write(&root.join("u/a.tif"), b"a");
        std::os::unix::fs::symlink(dir.path().join("outside.tif"), root.join("u/link.tif")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("outside.tif"), root.join("v")).unwrap();

        let scan = layout(&root, "").scan(None);
        assert_eq!(scan.units["u"].files.keys().collect::<Vec<_>>(), ["u/a.tif"]);
        assert!(scan.units["u"].errors[0].contains("symbolic links are not followed"));
        assert!(
            scan.errors[0].contains("symbolic links are not followed"),
            "a link at unit level"
        );

        let excluded = layout(&root, "    exclude: ['u/link.tif', 'v']\n").scan(None);
        assert!(excluded.units["u"].errors.is_empty() && excluded.errors.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_name_that_is_not_unicode_is_an_error() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("u/a.tif"), b"a");
        write(&root.join("u").join(std::ffi::OsStr::from_bytes(b"bad\xff.tif")), b"b");

        let scan = layout(root, "").scan(None);
        assert!(scan.units["u"].errors[0].contains("not valid Unicode"));
    }

    #[cfg(unix)]
    #[test]
    fn an_unread_directory_could_hold_any_regex_units_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        write(&root.join("RUN1_a.csv"), b"");
        write(&root.join("locked/RUN1_b.csv"), b"");
        let lay = layout(&root, "    unit: {regex: '(?:^|/)(?P<unit>RUN\\d+)_'}\n");

        let locked = lock_out(&root.join("locked"));
        let scan = lay.scan(None);
        let_in(&root.join("locked"));
        if !locked {
            return;
        }
        assert!(scan.unattributed);
        assert_eq!(scan.units["RUN1"].files.len(), 1);
    }
}
