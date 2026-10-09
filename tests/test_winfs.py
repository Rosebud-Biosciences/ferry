import sys

from ferry.winfs import held_open

from .helpers import write


def test_a_file_another_handle_holds_open_reads_as_busy_on_windows(tmp_path):
    path = write(tmp_path / "a.tif")
    with open(path, "ab"):
        # Only Windows can tell; elsewhere the probe never holds a file back.
        assert held_open(path) is (sys.platform == "win32")
    assert held_open(path) is False
