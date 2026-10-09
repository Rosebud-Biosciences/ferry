from pathlib import Path

from ferry import plugins
from ferry.adapters import Adapter
from ferry.config import load_config
from ferry.notify import Notifier
from ferry.registrars.base import Registrar

EXAMPLE = Path(__file__).parents[1] / "examples" / "ferry.example.yaml"


def test_the_example_config_validates_and_its_plugins_load():
    cfg = load_config(
        EXAMPLE,
        env={
            "FERRY_S3_KEY": "key",
            "FERRY_S3_SECRET": "secret",
            "FERRY_ALERT_WEBHOOK": "https://hooks.example/x",
        },
    )

    assert [(s.name, s.mode, s.upload) for s in cfg.sources] == [
        ("workcell1_cytation", "cron", "unit"),
        ("operetta", "stream", "file"),
        ("plate_reader", "stream", "unit"),
    ]
    for source in cfg.sources:
        for ref in source.registrars:
            plugins.build(ref, plugins.REGISTRARS, Registrar)
        if source.adapter is not None:
            plugins.build(source.adapter, plugins.ADAPTERS, Adapter)
    for ref in cfg.notifiers:
        plugins.build(ref, plugins.NOTIFIERS, Notifier)
