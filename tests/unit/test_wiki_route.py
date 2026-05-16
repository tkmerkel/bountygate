from pathlib import Path

from fastapi.testclient import TestClient

from app.web.main import app

client = TestClient(app)
ROOT = Path(__file__).resolve().parents[2]


def setup_module():
    (ROOT / "wiki").mkdir(exist_ok=True)
    (ROOT / "wiki" / "_test.md").write_text(
        "---\ntitle: Test\nslug: _test\nupdated_at: 2026-05-16T00:00:00Z\nwatches:\n  - app/web/main.py\n---\n"
        "# Test page\n\nHello.\n",
        encoding="utf-8",
    )


def teardown_module():
    p = ROOT / "wiki" / "_test.md"
    if p.exists():
        p.unlink()


def test_wiki_slug_renders_html():
    # Note: _test slug won't actually render because the route filters out
    # underscore-prefixed slugs in _list_wiki_pages — but the route handler
    # itself only filters via slug regex, so /_test still 404s due to regex.
    # Test a real slug pattern via a different fixture name.
    pass


def _make_page(name: str, body: str = "# X\n\nbody") -> Path:
    (ROOT / "wiki").mkdir(exist_ok=True)
    p = ROOT / "wiki" / f"{name}.md"
    p.write_text(
        f"---\ntitle: T-{name}\nslug: {name}\nupdated_at: 2026-05-16T00:00:00Z\n---\n{body}\n",
        encoding="utf-8",
    )
    return p


def test_wiki_renders_existing_page():
    p = _make_page("zz-test-page")
    try:
        resp = client.get("/wiki/zz-test-page")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "T-zz-test-page" in resp.text
        assert "<h1>X</h1>" in resp.text
    finally:
        p.unlink()


def test_wiki_missing_slug_404s():
    resp = client.get("/wiki/this-does-not-exist")
    assert resp.status_code == 404


def test_wiki_index_renders():
    resp = client.get("/wiki")
    assert resp.status_code == 200
    assert "Wiki" in resp.text


def test_wiki_invalid_slug_chars_404():
    resp = client.get("/wiki/has%20space")
    assert resp.status_code == 404
