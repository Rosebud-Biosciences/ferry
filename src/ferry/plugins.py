"""Loading plugins: built-in names, installed entry points, or ``module:attr``."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from importlib.metadata import entry_points
from typing import Any, TypeVar

from .config import PluginRef

T = TypeVar("T")

REGISTRARS = "ferry.registrars"
ADAPTERS = "ferry.adapters"
NOTIFIERS = "ferry.notifiers"

BUILTINS: dict[str, dict[str, str]] = {
    REGISTRARS: {
        "manifest": "ferry.registrars.manifest:ManifestRegistrar",
        "null": "ferry.registrars.base:NullRegistrar",
    },
    ADAPTERS: {"default": "ferry.adapters:Adapter"},
    NOTIFIERS: {"webhook": "ferry.notify:WebhookNotifier"},
}


class PluginError(RuntimeError):
    """A plugin could not be found, imported or constructed."""


def _import(spec: str) -> Any:
    module_name, _, attr = spec.partition(":")
    try:
        obj: Any = importlib.import_module(module_name)
    except ImportError as exc:
        raise PluginError(
            f"cannot import {module_name!r} for plugin {spec!r}: {exc}"
        ) from exc
    for part in attr.split(".") if attr else []:
        try:
            obj = getattr(obj, part)
        except AttributeError as exc:
            raise PluginError(f"{module_name!r} has no {attr!r}") from exc
    return obj


def resolve(spec: str, group: str, builtins: Mapping[str, str] | None = None) -> Any:
    builtins = BUILTINS.get(group, {}) if builtins is None else builtins
    if spec in builtins:
        return _import(builtins[spec])
    if ":" in spec:
        return _import(spec)
    found = entry_points(group=group, name=spec)
    for entry in found:
        try:
            return entry.load()
        except ImportError as exc:
            raise PluginError(f"cannot load {group} plugin {spec!r}: {exc}") from exc
    known = sorted({*builtins, *(ep.name for ep in entry_points(group=group))})
    raise PluginError(
        f"unknown {group} plugin {spec!r} (known: {', '.join(known) or 'none'}; "
        "or give module:attr)"
    )


def build(ref: PluginRef, group: str, base: type[T]) -> T:
    factory = resolve(ref.use, group)
    try:
        instance = factory(**ref.options)
    except TypeError as exc:
        raise PluginError(f"bad options for {group} plugin {ref.use!r}: {exc}") from exc
    if not isinstance(instance, base):
        raise PluginError(
            f"{group} plugin {ref.use!r} built a {type(instance).__name__}, "
            f"not a {base.__name__}"
        )
    return instance
