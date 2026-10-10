//! Glob matching over relative POSIX paths.
//!
//! `*` and `?` stay within one path component, `**/` matches any number of
//! leading directories, and a trailing `/**` everything below. A pattern
//! without a `/` matches a path's last component, as in gitignore: `*.tif`
//! selects TIFFs at any depth, while `images/*.tif` is anchored. Backslashes
//! are read as separators, so Windows-style patterns work unchanged, and
//! matching is case-insensitive on Windows, whose file systems are.

use anyhow::{Context, Result, bail};
use globset::{GlobBuilder, GlobMatcher};

pub const CASE_SENSITIVE: bool = !cfg!(windows);

pub fn normalize_pattern(pattern: &str) -> String {
    let mut text = pattern.trim().replace('\\', "/");
    while let Some(rest) = text.strip_prefix("./") {
        text = rest.to_string();
    }
    text.trim_start_matches('/').to_string()
}

#[derive(Debug, Clone)]
pub struct PathGlob {
    pub pattern: String,
    anchored: bool,
    matcher: GlobMatcher,
}

impl PathGlob {
    pub fn new(pattern: &str) -> Result<Self> {
        Self::with_case(pattern, CASE_SENSITIVE)
    }

    pub fn with_case(pattern: &str, case_sensitive: bool) -> Result<Self> {
        let text = normalize_pattern(pattern);
        if text.is_empty() {
            bail!("empty glob pattern: {pattern:?}");
        }
        let matcher = GlobBuilder::new(&text)
            .literal_separator(true)
            .backslash_escape(false)
            .case_insensitive(!case_sensitive)
            .build()
            .with_context(|| format!("invalid glob {pattern:?}"))?
            .compile_matcher();
        Ok(Self {
            anchored: text.contains('/'),
            pattern: text,
            matcher,
        })
    }

    pub fn is_match(&self, rel: &str) -> bool {
        if self.anchored {
            self.matcher.is_match(rel)
        } else {
            self.matcher.is_match(rel.rsplit('/').next().unwrap_or(rel))
        }
    }
}

/// Any-of matching over several patterns; an empty list matches nothing.
#[derive(Debug, Clone, Default)]
pub struct GlobList(Vec<PathGlob>);

impl GlobList {
    pub fn new<S: AsRef<str>>(patterns: &[S]) -> Result<Self> {
        patterns
            .iter()
            .map(|p| PathGlob::new(p.as_ref()))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }

    pub fn is_match(&self, rel: &str) -> bool {
        self.0.iter().any(|glob| glob.is_match(rel))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, path: &str) -> bool {
        PathGlob::with_case(pattern, true).unwrap().is_match(path)
    }

    #[test]
    fn glob_semantics() {
        let cases = [
            ("*.tif", "a.tif", true),
            ("*.tif", "a/b/c.tif", true),
            ("*.tif", "a/b/c.tiff", false),
            ("img/*.tif", "img/a.tif", true),
            ("img/*.tif", "x/img/a.tif", false),
            ("img/*.tif", "img/sub/a.tif", false),
            ("**/a.tif", "a.tif", true),
            ("**/a.tif", "x/y/a.tif", true),
            ("img/**", "img/x/y.tif", true),
            ("img/**", "imgs/x.tif", false),
            ("a/**/z", "a/z", true),
            ("a/**/z", "a/b/c/z", true),
            ("t00?.tif", "t001.tif", true),
            ("[!_]*.csv", "_hidden.csv", false),
            ("[!_]*.csv", "data.csv", true),
            ("img\\*.tif", "img/a.tif", true),
            ("./img/*.tif", "img/a.tif", true),
        ];
        for (pattern, path, expected) in cases {
            assert_eq!(matches(pattern, path), expected, "{pattern} vs {path}");
        }
    }

    #[test]
    fn case_folding_is_explicit() {
        assert!(PathGlob::with_case("*.TIF", false).unwrap().is_match("a.tif"));
        assert!(!PathGlob::with_case("*.TIF", true).unwrap().is_match("a.tif"));
    }

    #[test]
    fn empty_patterns_are_rejected() {
        assert!(PathGlob::new("  ").is_err());
        assert!(!GlobList::new::<&str>(&[]).unwrap().is_match("x"));
    }
}
