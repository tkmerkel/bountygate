from fastapi.testclient import TestClient

from app.web.main import app


def test_root_returns_service_json():
    client = TestClient(app)
    body = client.get("/").json()
    assert body["service"] == "bountygate read API"
    assert body["docs"] == "/docs"
