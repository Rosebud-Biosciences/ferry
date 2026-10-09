"""Alerts: things an operator should hear about, sent to notifier plugins.

Every alert is also logged. Notifiers run on a background thread so a slow or
unreachable webhook never stalls transfers, and one failing notifier does not
silence the others.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import urllib.request
from dataclasses import dataclass
from typing import Any, Literal

Level = Literal["info", "warning", "error"]
LEVELS: dict[str, int] = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


@dataclass(frozen=True)
class Alert:
    level: Level
    title: str
    message: str = ""
    source: str | None = None
    unit: str | None = None

    def text(self) -> str:
        return f"{self.title}\n{self.message}" if self.message else self.title


class Notifier:
    """Base class for notifier plugins (``notifiers: [{use: ..., options: ...}]``)."""

    def __init__(self, **options: Any) -> None:
        if options:
            raise TypeError(
                f"{type(self).__name__} takes no options; got {sorted(options)}"
            )

    def notify(self, alert: Alert) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return None


class WebhookNotifier(Notifier):
    """POST alerts as ``{"text": ...}`` JSON, the body Slack incoming webhooks
    (and Mattermost, Teams workflows, ...) accept."""

    def __init__(self, url: str, min_level: Level = "warning", timeout: float = 10.0):
        super().__init__()
        if min_level not in LEVELS:
            raise ValueError(f"min_level must be one of {sorted(LEVELS)}")
        self.url = url
        self.min_level = min_level
        self.timeout = timeout

    def notify(self, alert: Alert) -> None:
        if LEVELS[alert.level] < LEVELS[self.min_level]:
            return
        request = urllib.request.Request(
            self.url,
            data=json.dumps({"text": alert.text()}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout):
            pass


_STOP = object()


class Alerts:
    """Log alerts and hand them to notifiers on a background thread."""

    def __init__(self, notifiers: list[Notifier], logger: logging.Logger) -> None:
        self.notifiers = notifiers
        self.logger = logger
        self._queue: queue.SimpleQueue[object] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self.sent: list[Alert] = []

    def emit(self, alert: Alert) -> None:
        self.logger.log(
            LEVELS[alert.level],
            "%s",
            alert.text(),
            extra={"alert": True, "source": alert.source, "unit": alert.unit},
        )
        self.sent.append(alert)
        if not self.notifiers:
            return
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, name="ferry-alerts", daemon=True
            )
            self._thread.start()
        self._queue.put(alert)

    def _run(self) -> None:
        while True:
            alert = self._queue.get()
            if alert is _STOP:
                return
            assert isinstance(alert, Alert)
            for notifier in self.notifiers:
                try:
                    notifier.notify(alert)
                except Exception:
                    self.logger.warning(
                        "Notifier %s failed", type(notifier).__name__, exc_info=True
                    )

    def close(self, timeout: float = 15.0) -> None:
        if self._thread is not None:
            self._queue.put(_STOP)
            self._thread.join(timeout)
            self._thread = None
        for notifier in self.notifiers:
            try:
                notifier.close()
            except Exception:
                self.logger.warning("Closing %s failed", type(notifier).__name__)
