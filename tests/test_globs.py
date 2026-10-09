import pytest

from ferry.globs import GlobSet, PathGlob


@pytest.mark.parametrize(
    ("pattern", "path", "expected"),
    [
        # slashless patterns match the last component, at any depth
        ("*.tif", "a.tif", True),
        ("*.tif", "a/b/c.tif", True),
        ("*.tif", "a/b/c.tiff", False),
        # a slash anchors the pattern to the whole path
        ("img/*.tif", "img/a.tif", True),
        ("img/*.tif", "x/img/a.tif", False),
        ("img/*.tif", "img/sub/a.tif", False),
        # ** crosses directories, including none
        ("**/a.tif", "a.tif", True),
        ("**/a.tif", "x/y/a.tif", True),
        ("img/**", "img/x/y.tif", True),
        ("img/**", "imgs/x.tif", False),
        ("a/**/z", "a/z", True),
        ("a/**/z", "a/b/c/z", True),
        # ? and classes stay within a component
        ("t00?.tif", "t001.tif", True),
        ("t00?.tif", "t00/.tif", False),
        ("[!_]*.csv", "_hidden.csv", False),
        ("[!_]*.csv", "data.csv", True),
        # backslashes are separators, and leading ./ or / is ignored
        ("img\\*.tif", "img/a.tif", True),
        ("./img/*.tif", "img/a.tif", True),
    ],
)
def test_glob_semantics(pattern, path, expected):
    assert PathGlob(pattern, case_sensitive=True).match(path) is expected


def test_case_insensitive_matching_is_explicit():
    assert PathGlob("*.TIF", case_sensitive=False).match("a.tif")
    assert not PathGlob("*.TIF", case_sensitive=True).match("a.tif")


def test_a_glob_set_matches_any_and_an_empty_one_nothing():
    globs = GlobSet(["*.tif", "*.xml"], case_sensitive=True)
    assert globs.match("a/index.xml")
    assert not globs.match("a/notes.txt")
    assert not GlobSet([]).match("anything")


def test_an_empty_pattern_is_rejected():
    with pytest.raises(ValueError, match="empty glob"):
        PathGlob("  ")
