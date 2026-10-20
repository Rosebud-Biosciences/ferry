"""Unpack the ferry binary from each wheel into an archive of its own.

    python3 .github/scripts/package_binaries.py VERSION WHEELS OUT

WHEELS holds a `wheel-<target>/` directory per Rust target, as the release
workflow downloads its artifacts. Each archive -- `ferry-<version>-<target>`,
`.zip` for Windows and `.tar.gz` elsewhere -- holds the binary exactly as the
wheel does, with the wheel's CycloneDX SBOM, the license and the README.
"""

import io
import sys
import tarfile
import time
import zipfile
from pathlib import Path


def unpack(wheel: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(wheel) as whl:
        names = whl.namelist()
        (binary,) = [n for n in names if ".data/scripts/ferry" in n]
        files = {Path(binary).name: whl.read(binary)}
        for sbom in (n for n in names if ".dist-info/sboms/" in n):
            files[Path(sbom).name] = whl.read(sbom)
    return files


def main(version: str, wheels: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    folders = sorted(wheels.glob("wheel-*"))
    if not folders:
        sys.exit(f"no wheel-<target> directories in {wheels}")
    for folder in folders:
        target = folder.name.removeprefix("wheel-")
        (wheel,) = folder.glob("*.whl")
        files = unpack(wheel)
        files["LICENSE"] = Path("LICENSE").read_bytes()
        files["README.md"] = Path("README.md").read_bytes()
        stem = f"ferry-{version}-{target}"
        if "windows" in target:
            path = out / f"{stem}.zip"
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
                for name, data in files.items():
                    archive.writestr(f"{stem}/{name}", data)
        else:
            path = out / f"{stem}.tar.gz"
            with tarfile.open(path, "w:gz") as archive:
                for name, data in files.items():
                    info = tarfile.TarInfo(f"{stem}/{name}")
                    info.size = len(data)
                    info.mode = 0o755 if name == "ferry" else 0o644
                    info.mtime = int(time.time())
                    archive.addfile(info, io.BytesIO(data))
        print(path)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]))
