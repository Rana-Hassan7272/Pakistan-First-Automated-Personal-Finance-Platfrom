"""Phase 9 — ingest verification helpers and eval JSONL helpers (mocked Supabase / RPC)."""

from __future__ import annotations

import pytest

from backend.rag.evaluation.run_ragas import _reference_contexts_for_row
from backend.rag.ingest_verify import verify_ingest
from backend.rag.models import RagChildMatch


def test_reference_contexts_prefers_jsonl_list() -> None:
    row = {
        "ground_truth": "fallback gt",
        "reference_contexts": ["  first ref ", "", "second"],
    }
    assert _reference_contexts_for_row(row) == ["first ref", "second"]


def test_reference_contexts_falls_back_to_ground_truth() -> None:
    assert _reference_contexts_for_row({"ground_truth": "  only gt  "}) == ["only gt"]
    assert _reference_contexts_for_row({"ground_truth": ""}) == []


def test_verify_ingest_zero_children_skips_match_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.rag.ingest_verify._table_count",
        lambda _sb, _name: 0,
    )

    out = verify_ingest(object())
    assert out["rag_child_chunks"] == 0
    assert out.get("warning")
    assert out["match_rpc"] is None


def test_verify_ingest_runs_match_rpc_when_children_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_table_count(_sb: object, name: str) -> int:
        return {"rag_documents": 3, "rag_parent_chunks": 9, "rag_child_chunks": 40}[name]

    def fake_embed(_q: str) -> list[float]:
        return [0.02] * 384

    def fake_match(_sb: object, _qv: list[float], match_count: int = 5) -> list[RagChildMatch]:
        return [
            RagChildMatch(child_chunk_id="c1", similarity=0.91),
            RagChildMatch(child_chunk_id="c2", similarity=0.82),
        ]

    monkeypatch.setattr("backend.rag.ingest_verify._table_count", fake_table_count)
    monkeypatch.setattr("backend.rag.ingest_verify.embed_query_text", fake_embed)
    monkeypatch.setattr("backend.rag.ingest_verify.match_rag_child_chunks", fake_match)

    out = verify_ingest(object())
    assert out["rag_child_chunks"] == 40
    assert out["match_rpc"] is not None
    assert len(out["match_rpc"]) == 2
    assert out["match_rpc"][0]["child_chunk_id"] == "c1"
