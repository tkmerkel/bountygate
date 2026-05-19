from scripts.wiki_hook import affected_pages, parse_watches


def test_parse_watches_extracts_list(tmp_path):
    p = tmp_path / "p.md"
    p.write_text(
        "---\ntitle: X\nslug: x\nwatches:\n  - foo/bar.py\n  - baz/qux.py\nupdated_at: 2026-01-01\n---\nbody",
        encoding="utf-8",
    )
    assert parse_watches(p) == ["foo/bar.py", "baz/qux.py"]


def test_parse_watches_empty_when_missing(tmp_path):
    p = tmp_path / "p.md"
    p.write_text("---\ntitle: X\nslug: x\n---\nbody", encoding="utf-8")
    assert parse_watches(p) == []


def test_affected_pages_matches_on_watched_path(tmp_path):
    wiki = tmp_path / "wiki"
    wiki.mkdir()
    (wiki / "a.md").write_text(
        "---\nslug: a\nwatches:\n  - src/foo.py\n---\n", encoding="utf-8"
    )
    (wiki / "b.md").write_text(
        "---\nslug: b\nwatches:\n  - src/bar.py\n---\n", encoding="utf-8"
    )
    changed = ["src/foo.py", "README.md"]
    assert affected_pages(wiki, changed) == ["a"]


def test_affected_pages_handles_no_matches(tmp_path):
    wiki = tmp_path / "wiki"
    wiki.mkdir()
    (wiki / "a.md").write_text(
        "---\nslug: a\nwatches:\n  - src/foo.py\n---\n", encoding="utf-8"
    )
    assert affected_pages(wiki, ["src/unrelated.py"]) == []


def test_affected_pages_skips_underscore_prefixed(tmp_path):
    wiki = tmp_path / "wiki"
    wiki.mkdir()
    (wiki / "_draft.md").write_text(
        "---\nslug: _draft\nwatches:\n  - src/foo.py\n---\n", encoding="utf-8"
    )
    assert affected_pages(wiki, ["src/foo.py"]) == []


def test_affected_pages_returns_multiple_in_sorted_order(tmp_path):
    wiki = tmp_path / "wiki"
    wiki.mkdir()
    (wiki / "b.md").write_text(
        "---\nslug: b\nwatches:\n  - src/x.py\n---\n", encoding="utf-8"
    )
    (wiki / "a.md").write_text(
        "---\nslug: a\nwatches:\n  - src/x.py\n---\n", encoding="utf-8"
    )
    assert affected_pages(wiki, ["src/x.py"]) == ["a", "b"]
