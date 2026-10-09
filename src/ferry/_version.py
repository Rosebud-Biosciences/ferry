from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ferry-ingest")
except PackageNotFoundError:  # pragma: no cover - running from a bare checkout
    __version__ = "0+unknown"

AGENT = f"ferry-ingest/{__version__}"
