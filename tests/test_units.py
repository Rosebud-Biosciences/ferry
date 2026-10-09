import os

from ferry.config import SourceConfig
from ferry.units import Layout

from .helpers import age_tree, write


def layout(root, **source) -> Layout:
    return Layout(SourceConfig(name="inst", root=root, store="s", **source))


def test_directory_units_at_depth_two(tmp_path):
    write(tmp_path / "plates" / "p1__a" / "x.tif", b"xx")
    write(tmp_path / "plates" / "p1__a" / "deep" / "y.tif", b"y")
    write(tmp_path / "plates" / "skip" / "z.tif")
    write(tmp_path / "other" / "p2__b" / "z.tif")
    write(tmp_path / "loose.tif")

    scan = layout(tmp_path, unit={"dirs": "plates/*__*"}).scan()

    assert list(scan.units) == ["plates/p1__a"]
    unit = scan.units["plates/p1__a"]
    assert {rel: (f.within, f.size) for rel, f in unit.files.items()} == {
        "plates/p1__a/x.tif": ("x.tif", 2),
        "plates/p1__a/deep/y.tif": ("deep/y.tif", 1),
    }
    assert scan.scope is None


def test_excluded_entries_are_invisible_including_to_activity(tmp_path):
    write(tmp_path / "u" / "a.tif")
    write(tmp_path / "u" / "a.tif.tmp", age=0)  # still being written
    write(tmp_path / "u" / "$RECYCLE.BIN" / "junk.tif", age=0)
    age_tree(tmp_path / "u" / "a.tif")
    os.utime(tmp_path / "u", (1_000_000, 1_000_000))

    lay = layout(tmp_path, exclude=["*.tmp", "$RECYCLE.BIN"])
    unit = lay.scan().units["u"]

    assert list(unit.files) == ["u/a.tif"]
    # The newest non-excluded thing is a.tif: the excluded files did not count.
    assert unit.newest_ns == (tmp_path / "u" / "a.tif").stat().st_mtime_ns


def test_activity_counts_files_ferry_does_not_upload_and_directories(tmp_path):
    top = tmp_path / "u"
    write(top / "a.tif")
    write(top / "log.txt", age=5)
    os.utime(top, (1_000_000, 1_000_000))

    unit = layout(tmp_path, include=["*.tif"]).scan().units["u"]

    assert list(unit.files) == ["u/a.tif"]
    assert unit.newest_ns == (top / "log.txt").stat().st_mtime_ns

    # Adding an entry moves the directory's mtime, which counts as activity.
    os.utime(top / "log.txt", (1_000_000, 1_000_000))
    (top / "new_dir").mkdir()
    unit = layout(tmp_path, include=["*.tif"]).scan().units["u"]
    assert unit.newest_ns == max(
        top.stat().st_mtime_ns, (top / "new_dir").stat().st_mtime_ns
    )
    assert unit.newest_ns > (top / "a.tif").stat().st_mtime_ns


def test_markers_are_found_whether_or_not_they_are_uploaded(tmp_path):
    write(tmp_path / "u" / "images" / "index.xml")
    write(tmp_path / "v" / "images" / "a.tiff")

    scan = layout(tmp_path, include=["*.tiff"], marker="images/index.xml").scan()

    assert scan.units["u"].marker is True
    assert scan.units["u"].files == {}
    assert scan.units["v"].marker is False


def test_scoped_scans_report_what_they_looked_at(tmp_path):
    write(tmp_path / "a" / "x.tif")
    write(tmp_path / "b" / "x.tif")

    scan = layout(tmp_path).scan(["a", "gone"])

    assert list(scan.units) == ["a"]
    assert scan.scope == frozenset({"a", "gone"})


def test_regex_units_gather_files_from_anywhere(tmp_path):
    write(tmp_path / "RUN1_quant.csv")
    write(tmp_path / "2026" / "RUN1_melt.csv")
    write(tmp_path / "RUN2_quant.csv")
    write(tmp_path / "README")

    scan = layout(tmp_path, unit={"regex": r"(?:^|/)(?P<unit>RUN\d+)_"}).scan()

    assert sorted(scan.units) == ["RUN1", "RUN2"]
    assert {f.within for f in scan.units["RUN1"].files.values()} == {
        "RUN1_quant.csv",
        "2026/RUN1_melt.csv",
    }


def test_key_for_maps_event_paths_to_units(tmp_path):
    by_dirs = layout(tmp_path, unit={"dirs": "*/run*"})
    assert by_dirs.key_for("plate1/run2/a/b.tif") == "plate1/run2"
    assert by_dirs.key_for("plate1/run2") == "plate1/run2"
    assert by_dirs.key_for("plate1/other/b.tif") is None
    assert by_dirs.key_for("plate1") is None
    by_name = layout(tmp_path, unit={"regex": r"^(?P<unit>[^_]+)_"})
    assert by_name.key_for("RUN7_a.csv") == "RUN7"
    assert by_name.key_for("notes.txt") is None


def test_unit_keys_lists_unit_directories_without_walking_them(tmp_path):
    write(tmp_path / "a__1" / "x.tif")
    write(tmp_path / "b__2" / "deep" / "x.tif")
    write(tmp_path / "plain" / "x.tif")

    assert layout(tmp_path, unit={"dirs": "*__*"}).unit_keys() == ["a__1", "b__2"]
