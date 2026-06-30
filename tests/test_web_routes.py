from fastapi.testclient import TestClient

import web.main
from web.main import app


def test_legacy_nav_aliases_redirect(monkeypatch):
    monkeypatch.setattr(
        web.main,
        "get_auth_session",
        lambda request: {"username": "qa", "name": "QA", "role": "admin"},
    )
    client = TestClient(app)

    stats = client.get("/stats", follow_redirects=False)
    finder = client.get("/comp-finder", follow_redirects=False)

    assert stats.status_code == 303
    assert stats.headers["location"] == "/analytics"
    assert finder.status_code == 303
    assert finder.headers["location"] == "/finder"


def test_version_endpoint_is_public_and_reports_commit(monkeypatch):
    from fastapi.testclient import TestClient
    from web.main import app
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "abc123sha")
    client = TestClient(app)
    resp = client.get("/version")
    assert resp.status_code == 200          # public (not redirected to /login)
    assert resp.json()["commit"] == "abc123sha"
