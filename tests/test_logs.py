import json
import logging
from datetime import date, timedelta

from obstore.store import LocalStore

from ferry.journal import Journal
from ferry.logs import DailyFileHandler, log_files, ship_logs
from ferry.manifest import Manifest, ShippedLog, TransferEvent
from ferry.registrars.base import Registrar
from ferry.stores import Location


def emit(handler: logging.Handler, message: str, **extra) -> None:
    logger = logging.getLogger("ferry.test-logs")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        logger.info(message, extra=extra)
    finally:
        logger.removeHandler(handler)
        handler.close()


def test_each_record_is_one_json_line_with_its_extras(tmp_path):
    emit(
        DailyFileHandler(tmp_path, keep_days=3),
        "Uploaded a.tif",
        source="inst",
        bytes=5,
    )

    ((day, path),) = log_files(tmp_path)
    record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert day == date.today()
    assert {
        k: record[k] for k in ("level", "logger", "message", "source", "bytes")
    } == {
        "level": "INFO",
        "logger": "ferry.test-logs",
        "message": "Uploaded a.tif",
        "source": "inst",
        "bytes": 5,
    }


def test_days_older_than_keep_days_are_pruned(tmp_path):
    for days in (1, 2, 5):
        stamp = (date.today() - timedelta(days=days)).isoformat()
        (tmp_path / f"ferry-{stamp}.jsonl").write_text("{}\n", encoding="utf-8")

    emit(DailyFileHandler(tmp_path, keep_days=3), "hello")

    assert [day for day, _ in log_files(tmp_path)] == [
        date.today() - timedelta(days=2),
        date.today() - timedelta(days=1),
        date.today(),
    ]


class LogCatalog(Registrar):
    def __init__(self) -> None:
        super().__init__()
        self.logs: list[ShippedLog] = []

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        return None

    def register_log(self, log: ShippedLog) -> None:
        self.logs.append(log)


async def test_finished_days_ship_once_and_reach_the_registrars(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    (log_dir / f"ferry-{yesterday}.jsonl").write_text('{"a": 1}\n', encoding="utf-8")
    (log_dir / f"ferry-{date.today().isoformat()}.jsonl").write_text(
        "{}\n", encoding="utf-8"
    )
    journal = Journal(tmp_path / "journal.sqlite3")
    location = Location(
        LocalStore(prefix=tmp_path / "remote", mkdir=True), "_logs", "mem://_logs"
    )
    catalog = LogCatalog()

    async def ship() -> list[ShippedLog]:
        return await ship_logs(
            journal=journal,
            location=location,
            log_dir=log_dir,
            node="n1",
            registrars=[catalog],
        )

    shipped = await ship()
    again = await ship()
    journal.close()

    assert [log.name for log in shipped] == [f"ferry-{yesterday}.jsonl"]
    assert (
        tmp_path / "remote" / "_logs" / "n1" / f"ferry-{yesterday}.jsonl"
    ).read_text(encoding="utf-8") == '{"a": 1}\n'
    assert catalog.logs == shipped
    assert again == []
