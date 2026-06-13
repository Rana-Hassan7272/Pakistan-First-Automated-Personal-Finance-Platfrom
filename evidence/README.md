# FinGuard evidence folder

Pre-deployment benchmarks, eval logs, and ML metrics for FYP / CV.  
**Date range:** mostly 2026-06-01.  
**Do not commit secrets** — logs may mention API hosts only.

For **numbers to quote**, open `MLFLOW_METRICS.md` first, then the specific artifact below.

---

## Start here

| File | Use |
|------|-----|
| `MLFLOW_METRICS.md` | Curated metrics from MLflow + local runs (BERT, LSTM, fraud, production snapshot, harness) |
| `plan.md` | Original checklist of what to run before deploy (reference only) |
| This `README.md` | Index of every evidence file |

---

## Quality gates

| File | What was run | Notes |
|------|----------------|-------|
| `pytest_results.txt` | `pytest backend/tests tests -q` | 304 passed, 1 failed, 1 skipped. Failure: `test_electronics_merchant_not_classified_as_transfer` |
| `parser_eval_full.json` | `provider_pattern_eval.py --json` on 235 golden samples | Overall pass rate and per-provider breakdown — open JSON |
| `parser_eval_verbose.txt` | Same harness `--verbose` | Remaining failures: Allied/HBL/Meezan Gmail, UBL SMS, DLQ negatives |

---

## RAG knowledge base

| File | What was run | Notes |
|------|----------------|-------|
| `rag_kb_verify.txt` | `python -m backend.rag.scripts.ingest_kb --verify-only` | Doc/chunk counts + `match_rag_child_chunks` RPC smoke test |

Requires `backend/rag/indices/bm25_child.pkl` + `manifest.json` on the machine running hybrid RAG (not stored in this folder).

---

## RAG A/B/C evaluation

Two runs exist — **use the re-run for the report.**

### Official (Google Gemini primary + BM25 + Groq for router/grounding)

| Path | Contents |
|------|----------|
| `rag_eval_rerun/compare.md` | **Main table** — configs A / B / C |
| `rag_eval_rerun/summary_A.json` | Dense-only aggregates |
| `rag_eval_rerun/summary_B.json` | Hybrid (BM25 + dense) — best cosine on this run |
| `rag_eval_rerun/summary_C.json` | Full pipeline (hybrid + Self-RAG) |
| `rag_eval_rerun/results_*.jsonl` | Per-question answers, contexts, warnings |
| `rag_eval_rerun/run.log` | Full console log (if present) |

Command: `python -m backend.rag.evaluation.run_ragas --config all --limit 40 --output-dir evidence/rag_eval_rerun`

### Archive (first run — Groq primary, BM25 often missing)

| Path | Contents |
|------|----------|
| `rag_eval/compare.md` | Older A/B/C table — do not prefer over `rag_eval_rerun` |
| `rag_eval/summary_*.json`, `rag_eval/results_*.jsonl` | Same structure as re-run |

---

## ML models

| File | What was run | Notes |
|------|----------------|-------|
| `lstm_backtest.txt` | `python -m backend.ml.lstm.run_pipeline backtest` | Per-category MAE%; only `charity_donations` flagged for retrain |
| `fraud_eval.txt` | `python -m backend.ml.anomaly.run_pipeline eval` | Offline test set — precision/recall/F1/AUPRC; gate PASS |
| `fraud_mlflow.txt` | `python backend/ml/production_mlflow_weekly.py` | Log line only; KPIs live in MLflow run `production_weekly_20260601_17` |

Training-laptop MLflow runs (BERT train, LSTM train, etc.) are summarized in `MLFLOW_METRICS.md`, not duplicated here.

---

## Multi-agent tool harness

| File | What was run | Notes |
|------|----------------|-------|
| `agent_harness_post_llm.txt` | **Canonical** — after LLM routing change (Google RAG + OpenRouter agents) |
| `agent_harness.txt` | Earlier run (4/5 before `rank_bm25` / fixes) |
| `agent_harness_rerun.txt` | Optional duplicate if you re-ran after `pip install rank-bm25` |

Command: `python backend/agents/tools/test_harness.py <user_uuid>`

User used: `77f69279-173c-4286-8f27-7f45d8d55f34`

---

## Optional ops / latency (scripts in `evaluation/`)

| File | Command | Notes |
|------|---------|--------|
| `bert_inference_latency.txt` | `python evaluation/bert_inference_latency.py -n 100` | BERT p50/p95/p99 ms |
| `prometheus.txt` | `python evaluation/collect_ops_evidence.py` | API must be running |
| `health.json` | same script | GET /health |
| `advisor_latency_smoke.txt` | `python evaluation/advisor_latency_smoke.py -n 10` | Advisor p50/p95/p99 with API running |
| `lstm_charity_donations_retrain.txt` | (from backtest) | Retrain flag note — not a blocker |

---

## LangSmith (advisor latency)

| File | Contents |
|------|----------|
| `langsmith_advisor_latency.txt` | p50 **~4 s**, p99 **47.69 s** (`finguard-agents` project) |

Screenshots of traces are optional for FYP; not stored here unless you add them.

---

## Not in this folder (by design)

- App screenshots / screen recordings — after deploy
- `METRICS_SUMMARY.md` one-pager — optional; `MLFLOW_METRICS.md` already fills this role
- MLflow UI data — `backend/ml/mlruns/` (open with `python backend/ml/bert/open_mlflow.py 5000`)

---

## Quick “what to cite”

| Topic | Cite |
|-------|------|
| Automated tests | `pytest_results.txt` |
| PK bank parsers | `parser_eval_full.json` |
| RAG infrastructure | `rag_kb_verify.txt` |
| RAG quality comparison | `rag_eval_rerun/compare.md` |
| Spending forecasts | `lstm_backtest.txt` |
| Fraud ML offline | `fraud_eval.txt` |
| Live DB KPIs | MLflow `production_weekly_20260601_17` (+ `MLFLOW_METRICS.md`) |
| Advisor tools | `agent_harness_post_llm.txt` |
| Advisor latency (p50/p99) | `langsmith_advisor_latency.txt` |
| All headline numbers | `MLFLOW_METRICS.md` |

---

## Reproduce (from repo root, Git Bash)

```bash
export PYTHONPATH=.
# See plan.md for full sequence; summaries above match those commands.
```

LLM stack at time of evidence: RAG answers → Google `gemini-3.1-flash-lite` (10 RPM); agents → OpenRouter; Gmail parser fallback → Groq.
