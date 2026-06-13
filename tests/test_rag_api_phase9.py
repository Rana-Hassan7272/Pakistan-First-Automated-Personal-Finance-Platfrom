"""Phase 9 — advisor HTTP surface for ``rag_only`` (mocked classifier + gateway)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.agents.router_llm import RouteResult
from backend.api.dependencies import auth as auth_dep
from backend.api.routers.advisor import router as advisor_router


@pytest.fixture
def advisor_app() -> FastAPI:
    app = FastAPI()
    app.include_router(advisor_router)
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def advisor_client(advisor_app: FastAPI) -> TestClient:
    return TestClient(advisor_app)


def test_advisor_chat_rag_only_streams_and_calls_rag(
    advisor_app: FastAPI,
    advisor_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_route(_msg: str, _ctx=None) -> RouteResult:
        return RouteResult(path="rag_only", intent="rag_only", confidence=0.99)

    def fake_call_rag(
        query: str,
        user_id: str | None = None,
        context: dict | None = None,
        *,
        trusted_rag_user_id: str | None = None,
    ) -> dict:
        captured["query"] = query
        captured["user_id"] = user_id
        captured["trusted_rag_user_id"] = trusted_rag_user_id
        return {
            "answer": "Stub RAG answer for Phase 9 API test.",
            "success": True,
            "sources": ["phase9-test-source.pdf"],
        }

    monkeypatch.setattr("backend.agents.router_llm.route_user_message", fake_route)
    monkeypatch.setattr("backend.agents.tools.gateway.call_rag", fake_call_rag)

    resp = advisor_client.post(
        "/api/v1/advisor/chat",
        json={"user_id": "body-user-id", "message": "What is zakat in islamic finance?"},
    )
    assert resp.status_code == 200
    assert resp.headers.get("x-path") == "rag_only"
    assert "Stub RAG answer" in resp.text
    assert captured.get("query")
    assert captured.get("user_id") == "body-user-id"
    assert captured.get("trusted_rag_user_id") is None


def test_advisor_chat_rag_only_passes_jwt_sub_to_call_rag(
    advisor_app: FastAPI,
    advisor_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_route(_msg: str, _ctx=None) -> RouteResult:
        return RouteResult(path="rag_only", intent="rag_only", confidence=0.99)

    def fake_call_rag(
        query: str,
        user_id: str | None = None,
        context: dict | None = None,
        *,
        trusted_rag_user_id: str | None = None,
    ) -> dict:
        captured["trusted_rag_user_id"] = trusted_rag_user_id
        return {"answer": "ok from stub RAG with enough length for validator.", "success": True, "sources": ["phase9-test.pdf"]}

    monkeypatch.setattr("backend.agents.router_llm.route_user_message", fake_route)
    monkeypatch.setattr("backend.agents.tools.gateway.call_rag", fake_call_rag)

    advisor_app.dependency_overrides[auth_dep.optional_bearer_user_id] = lambda: "jwt-sub-from-test"
    resp = advisor_client.post(
        "/api/v1/advisor/chat",
        json={"user_id": "body-user-id", "message": "Explain riba in islamic banking."},
    )
    assert resp.status_code == 200
    assert captured.get("trusted_rag_user_id") == "jwt-sub-from-test"
