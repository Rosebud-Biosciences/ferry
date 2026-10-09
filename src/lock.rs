//! One agent per state directory.
//!
//! Two agents sharing a journal would both upload, register and move the same
//! files, so `ferry run` and `ferry watch` take an OS-level lock that the OS
//! drops if the process dies.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

pub struct StateLock {
    file: File,
    pub path: PathBuf,
}

impl StateLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => bail!(
                "another ferry agent is using {} (lock {})",
                path.parent().unwrap_or(path).display(),
                path.display()
            ),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        file.flush()?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_agent_on_the_same_state_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ferry.lock");
        let held = StateLock::acquire(&path).unwrap();
        let err = StateLock::acquire(&path).err().unwrap().to_string();
        assert!(err.contains("another ferry agent"), "{err}");
        drop(held);
        StateLock::acquire(&path).unwrap();
    }
}
