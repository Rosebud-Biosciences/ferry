"""ferry: ship instrument data from watched folders to S3-compatible storage.

The plugin surface is re-exported here: :class:`Registrar` (catalogs),
:class:`Adapter` (instrument metadata) and :class:`Notifier` (alerts), and the
:class:`Manifest` / :class:`TransferEvent` records they receive.
"""

from ._version import __version__
from .adapters import Adapter, UnitFile, UnitView
from .config import FerryConfig, SourceConfig, load_config
from .manifest import Manifest, ManifestFile, ShippedLog, TransferEvent
from .notify import Alert, Notifier
from .registrars.base import NullRegistrar, Registrar, RegistrarContext
from .runner import Agent

__all__ = [
    "Adapter",
    "Agent",
    "Alert",
    "FerryConfig",
    "Manifest",
    "ManifestFile",
    "Notifier",
    "NullRegistrar",
    "Registrar",
    "RegistrarContext",
    "ShippedLog",
    "SourceConfig",
    "TransferEvent",
    "UnitFile",
    "UnitView",
    "__version__",
    "load_config",
]
