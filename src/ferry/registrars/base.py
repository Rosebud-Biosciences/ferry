"""The registrar interface.

A registrar records completed units somewhere: a catalog database, a manifest
object, a lineage service. ferry calls it through a durable outbox, so a
registrar that is down only delays registration -- uploads carry on, and
nothing is finalized (moved or deleted) until every *required* registrar has
confirmed the unit.

Delivery is at-least-once: a crash between a call returning and ferry recording
it means the call is made again. ``register`` and ``finalize`` must therefore
be idempotent upserts keyed on ``manifest.uri``. A unit that changes after it
was registered is registered again with a higher ``manifest.generation``.

Every method runs on a worker thread, one call at a time per source, so a
blocking client is fine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from ferry.config import SourceConfig, StoreConfig
from ferry.manifest import Manifest, ShippedLog, TransferEvent
from ferry.stores import Location


@dataclass(frozen=True)
class RegistrarContext:
    node: str
    source: SourceConfig
    # The source's remote base (store + prefix); manifest URIs live under it.
    location: Location
    store: StoreConfig
    logger: logging.Logger


class Registrar:
    """Base class: override what your catalog needs.

    Options from the config (``registrars: [{use: ..., options: {...}}]``) are
    passed to ``__init__`` as keyword arguments; :meth:`open` then receives the
    source it serves. Nothing is opened for a dry run.
    """

    ctx: RegistrarContext

    def __init__(self, **options: Any) -> None:
        if options:
            raise TypeError(
                f"{type(self).__name__} takes no options; got {sorted(options)}"
            )

    def open(self, ctx: RegistrarContext) -> None:
        self.ctx = ctx

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        """Record a complete, fully uploaded unit (``transfers``: its uploads)."""
        raise NotImplementedError

    def finalize(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        """The source copies were kept, moved (see ``backup_path``) or deleted."""

    def record_failures(self, transfers: list[TransferEvent]) -> None:
        """Failed transfers, reported once each, for an audit trail."""

    def register_log(self, log: ShippedLog) -> None:
        """A day's agent log has been shipped to the store."""

    def close(self) -> None:
        """Release connections; called once at shutdown."""


class NullRegistrar(Registrar):
    """Records nothing: for sources whose data needs no catalog."""

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        return None
