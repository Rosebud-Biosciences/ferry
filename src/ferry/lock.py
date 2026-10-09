"""One agent per state directory.

Two agents sharing a journal would both upload, register and move the same
files, so ``ferry run`` / ``ferry watch`` take an OS-level lock that the OS
drops if the process dies.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import IO


class AlreadyRunning(RuntimeError):
    """Another ferry agent holds the state directory."""


class StateLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: IO[str] | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+", encoding="utf-8")  # noqa: SIM115 - held for the run
        try:
            if sys.platform == "win32":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            raise AlreadyRunning(
                f"another ferry agent is using {self.path.parent} (lock {self.path})"
            ) from exc
        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> StateLock:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()
