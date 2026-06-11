from fastapi.testclient import TestClient

from app.web.main import app

client = TestClient(app)


def test_index_served_at_root():
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert 'id="view-cross"' in r.text
    assert 'id="view-markets"' in r.text


def test_app_js_served():
    r = client.get("/static/app.js")
    assert r.status_code == 200
    assert "javascript" in r.headers["content-type"]


def test_styles_css_served():
    r = client.get("/static/styles.css")
    assert r.status_code == 200
    assert "css" in r.headers["content-type"]
