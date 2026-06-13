"""Phase 9 RAG ingest helpers — unit smoke tests."""

from __future__ import annotations

import os

import pytest

from backend.rag.chunking import split_parent_child
from backend.rag.manifest_catalog import infer_category_from_filename


def test_infer_zakat_filename():
    assert (
        infer_category_from_filename("how_to_calculate_the_zakat_of_your_money.pdf")
        == "islamic_finance"
    )


def test_infer_tax_filename():
    assert infer_category_from_filename("Tax on Fiverr & Upwork Income Pakistan.pdf") == "pakistani_tax"


def test_infer_fraud_filename():
    assert infer_category_from_filename("Banking fraud.pdf") == "fraud_security"


def test_chunking_produces_children():
    text = ("This is a sentence about zakat and nisab in Pakistan. " * 80).strip()
    pairs = split_parent_child(text)
    assert len(pairs) >= 1
    assert all(len(p.child_text) >= 20 for p in pairs)


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_SUPABASE") != "1",
    reason="Set RUN_LIVE_SUPABASE=1 with valid Supabase env to run live verify.",
)
def test_verify_ingest_live():
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.rag.ingest_pipeline import verify_ingest

    sb = get_supabase_admin_client()
    out = verify_ingest(sb)
    assert "rag_documents" in out


def test_rrf_orders_fused_ids():
    from backend.rag.hybrid_retriever import reciprocal_rank_fusion

    dense = ["a", "b", "c"]
    sparse = ["b", "d", "a"]
    scores = reciprocal_rank_fusion(dense, sparse)
    fused = sorted(scores.keys(), key=lambda i: scores[i], reverse=True)
    assert fused[0] == "b"


def test_rrf_dense_empty_sparse_only():
    from backend.rag.hybrid_retriever import reciprocal_rank_fusion

    scores = reciprocal_rank_fusion([], ["x", "y"])
    fused = sorted(scores.keys(), key=lambda i: scores[i], reverse=True)
    assert fused == ["x", "y"]


def test_router_zakat_bucket():
    from backend.rag.router import infer_filter_categories

    cats = infer_filter_categories("How do I calculate zakat on gold in Pakistan?")
    assert cats is not None
    assert "islamic_finance" in cats


def test_router_unmatched_returns_none():
    from backend.rag.router import infer_filter_categories

    assert infer_filter_categories("asdf qwerty zxcv random unmatched phrase") is None


def test_chunking_empty_input():
    assert split_parent_child("") == []
    assert split_parent_child("   \n\t  ") == []


def test_groq_aux_chain_uses_second_model(monkeypatch):
    pytest.importorskip("groq")
    import groq as groq_mod

    class FakeMsg:
        content = "OK"

    class FakeChoice:
        message = FakeMsg()

    class FakeResp:
        choices = [FakeChoice()]

    class FakeCompletions:
        def create(self, **kwargs):
            if kwargs["model"] == "bad-model":
                raise RuntimeError("simulated primary failure")
            return FakeResp()

    class FakeChat:
        completions = FakeCompletions()

    class FakeClient:
        def __init__(self, api_key=None):
            self.chat = FakeChat()

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr(groq_mod, "Groq", FakeClient)

    from backend.rag.groq_aux import groq_chat_model_chain

    txt, warn = groq_chat_model_chain(
        "system",
        "user",
        models=("bad-model", "good-model"),
        temperature=0.0,
        max_tokens=8,
    )
    assert txt == "OK"
    assert "rag_aux_groq_fallback_used" in warn


def test_web_search_unavailable_message_is_documented():
    from backend.rag.web_fallback import web_search_unavailable_user_message

    msg = web_search_unavailable_user_message()
    assert len(msg) > 80
    assert "web" in msg.lower() or "search" in msg.lower()


def test_embed_query_text_empty_returns_empty_list():
    from backend.rag.embedder import embed_query_text

    assert embed_query_text("") == []
    assert embed_query_text("  \t") == []


def test_embed_query_text_lru_reduces_embed_texts(monkeypatch):
    import backend.rag.config as rag_config

    monkeypatch.setattr(rag_config, "RAG_EMBED_QUERY_CACHE_SIZE", 8)

    calls: list[int] = []

    def fake_embed_texts(texts, batch_size=32):
        calls.append(len(texts))
        return [[float((j % 9 + 1) / 10.0) for j in range(384)] for _ in texts]

    monkeypatch.setattr("backend.rag.embedder.embed_texts", fake_embed_texts)
    from backend.rag.embedder import clear_query_embedding_cache, embed_query_text

    clear_query_embedding_cache()
    embed_query_text(" same question ")
    embed_query_text("same question")
    embed_query_text("other")
    assert calls == [1, 1]


def test_embed_query_text_cache_off_always_encodes(monkeypatch):
    import backend.rag.config as rag_config

    monkeypatch.setattr(rag_config, "RAG_EMBED_QUERY_CACHE_SIZE", 0)
    calls = 0

    def fake_embed_texts(texts, batch_size=32):
        nonlocal calls
        calls += 1
        return [[0.0] * 384 for _ in texts]

    monkeypatch.setattr("backend.rag.embedder.embed_texts", fake_embed_texts)
    from backend.rag.embedder import embed_query_text

    embed_query_text("x")
    embed_query_text("x")
    assert calls == 2
