"""The built-in ``manifest`` registrar: a JSON manifest per unit, in the store."""

from __future__ import annotations

from ferry.manifest import Manifest, TransferEvent

from .base import Registrar, RegistrarContext


class ManifestRegistrar(Registrar):
    """Write each unit's manifest as JSON next to its data.

    ``key`` is a template relative to the source's remote base, with ``{unit}``,
    ``{source}`` and ``{node}`` (default ``{unit}/_ferry_manifest.json``). The
    manifest lists every file's size and SHA-256, so it doubles as an integrity
    record, and a bucket notification on its key is a catalog-agnostic signal
    that a unit is complete. It is rewritten on finalize, with backup paths.
    """

    def __init__(
        self, key: str = "{unit}/_ferry_manifest.json", indent: int | None = 2
    ) -> None:
        super().__init__()
        self.key = key
        self.indent = indent

    def open(self, ctx: RegistrarContext) -> None:
        super().open(ctx)
        self.location = ctx.location

    def _write(self, manifest: Manifest) -> None:
        rel = self.key.format(
            unit=manifest.unit, source=manifest.source, node=manifest.node
        )
        body = manifest.to_json(indent=self.indent).encode()
        self.location.store.put(self.location.key(rel), body)

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        self._write(manifest)

    def finalize(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        self._write(manifest)
