"""Windows-only file checks (no-ops elsewhere)."""

from __future__ import annotations

import sys
from pathlib import Path


def held_open(path: Path) -> bool:
    """Whether another process holds ``path`` open, on Windows.

    Opens the file with no sharing allowed: a sharing violation means a writer
    (or reader) still has it. Instrument software often keeps a file open for
    its whole acquisition while leaving the mtime alone, which the settle check
    alone would read as finished.
    """
    if sys.platform != "win32":
        return False
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel32.CreateFileW
    create.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create.restype = wintypes.HANDLE
    generic_read, open_existing, normal = 0x80000000, 3, 0x80
    handle = create(str(path), generic_read, 0, None, open_existing, normal, None)
    if handle == wintypes.HANDLE(-1).value:
        return ctypes.get_last_error() == 32  # ERROR_SHARING_VIOLATION
    kernel32.CloseHandle(handle)
    return False
