//! Windows-only file checks (no-ops elsewhere).

use std::path::Path;

/// Whether another process holds `path` open, on Windows.
///
/// Opens the file with no sharing allowed: a sharing violation means a writer
/// (or reader) still has it. Instrument software often keeps a file open for a
/// whole acquisition while leaving its mtime alone, which the settle check
/// alone would read as finished.
#[cfg(windows)]
pub fn held_open(path: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_SHARING_VIOLATION, GENERIC_READ, GetLastError, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{CreateFileW, FILE_ATTRIBUTE_NORMAL, OPEN_EXISTING};

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: `wide` is a NUL-terminated UTF-16 path that outlives the call;
    // a valid handle is closed before returning.
    unsafe {
        let handle = CreateFileW(
            wide.as_ptr(),
            GENERIC_READ,
            0,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        );
        if handle == INVALID_HANDLE_VALUE {
            return GetLastError() == ERROR_SHARING_VIOLATION;
        }
        CloseHandle(handle);
    }
    false
}

#[cfg(not(windows))]
pub fn held_open(_path: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_another_handle_holds_open_reads_as_busy_on_windows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.tif");
        std::fs::write(&path, b"x").unwrap();
        {
            let _held = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            // Only Windows can tell; elsewhere the probe never holds a file back.
            assert_eq!(held_open(&path), cfg!(windows));
        }
        assert!(!held_open(&path));
    }
}
