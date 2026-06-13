I went through `latest-changes.md`, `update.md` (Phase 9–14 / FYP notes), `README.md`, `backend/API_REFERENCE.md`, and the evaluation/ML/metrics code. Below is a **pre-deployment evidence plan** tailored for **FYP + CV** — what to run, what numbers to save, and what to screenshot.

---

## What you already have (use it — don’t reinvent)

| Area | Where it lives | What it gives you |
|------|----------------|-------------------|
| **Parser regression** | `evaluation/ingestion/provider_pattern_eval.py` + **~235** lines in `golden_samples.jsonl` | Overall pass %, per-provider pass %, direction P/R/F1, field accuracy |
| **Unit/integration tests** | `backend/tests/` (~70+ files) + `tests/test_rag_*.py` | Automated correctness (ledger, parsers, zakat, fraud, spend context, etc.) |
| **RAG evaluation** | `backend/rag/evaluation/run_ragas.py`, `compare_configs.py`, `ragas_questions.jsonl` (~40 Qs) | Configs **A / B / C** comparison (dense vs hybrid vs full production) |
| **MLflow (unified)** | `backend/ml/mlflow_tracking.py`, `backend/ml/mlruns/` | BERT, LSTM, RAG eval, production snapshots in one store |
| **LSTM backtest** | `backend/ml/lstm/backtest.py`, `run_pipeline.py backtest` | Per-category MAE/RMSE vs actuals (strong FYP story) |
| **Anomaly / fraud** | `backend/ml/anomaly/run_pipeline.py` (`train`, `eval`) | Model metrics + production FP rate via `production_mlflow_weekly.py` |
| **Production KPIs** | `backend/ml/production_mlflow_weekly.py` | BERT correction rate, fraud FP rate, agent run stats |
| **Prometheus** | `GET /metrics` via `backend/api/core/metrics.py` | Queue time, prefilter rejection, circuit breaker, OCR load |
| **Admin / parser API** | `backend/api/routes/admin_parser.py` | `/run-evaluation`, `/field-accuracy`, health by pass rate |
| **Agent tool harness** | `backend/agents/tools/test_harness.py` | 5 live scenarios (DB, LSTM, RAG, computation, fraud) |
| **Load sketch** | `backend/rag/evaluation/locustfile.py` | Advisor/RAG latency under load |
| **ETL data quality** | `backend/etl/data_quality.py` (Great Expectations) | Batch validation logged to MLflow |
| **RAG KB verify** | `python -m backend.rag.scripts.ingest_kb --verify-only` | Document/chunk counts before deploy |

`update.md` explicitly calls out FYP evidence: RAG A/B/C table, LSTM monthly backtest, fraud FP rate, LangSmith traces, parser pass rate, screenshots of every major feature.

---

## Before deployment — do these in order

### 1. Quality gates (must pass — block deploy if bad)

**A. Automated tests (save full log + summary line)**

```powershell
cd d:\finguard
$env:PYTHONPATH="."
py -3 -m pytest backend/tests tests -q --tb=no 2>&1 | Tee-Object -FilePath evidence\pytest_results.txt
```

Record: **total passed / failed / skipped**, runtime, date.

**B. Parser golden harness (core differentiator for PK banks)**

```powershell
py -3 evaluation\ingestion\provider_pattern_eval.py --json -o evidence\parser_eval_full.json
py -3 evaluation\ingestion\provider_pattern_eval.py --verbose -o evidence\parser_eval_failures.txt
```

Record and put in report:

- **Overall pass rate** (target: document ≥90% overall; per critical provider ≥85%)
- **Per provider × source** (sms / notification / gmail / pdf): pass %, direction F1
- **Field accuracy** breakdown (amount, direction, merchant, date)
- Count of golden samples (**~235**)

Optional: filter the six Home providers and report separately (JazzCash, EasyPaisa, Allied, Askari, Meezan, UBL).

**C. RAG KB integrity (production server or local with env)**

```powershell
py -3 -m backend.rag.scripts.ingest_kb --verify-only
```

Screenshot/log: document count, parent/child chunk counts, corpus hash.

**D. Pending DB migrations**

From `latest-changes.md`: at minimum `20260531_investment_ledger.sql`, `20260531_spend_context_gps.sql`, plus any not applied on prod Supabase.

---

### 2. ML / AI evidence (CV + FYP “research” section)

**A. BERT categorization (layer 7)**

If you have a trained run in MLflow or can run eval:

- **Weighted F1**, **macro F1**, **accuracy**
- Per-class precision/recall/F1 (`backend/ml/bert/metrics.py`)
- **Inference latency** (p50/p95 ms) — even a small script over 100 sample texts is enough

`update.md` targets: correction rate **&lt;10% per category** in production (measure via `production_mlflow_weekly.py`).

**B. LSTM / predictions**

```powershell
py -3 -m backend.ml.lstm.run_pipeline backtest
```

Record per category: **MAE %**, **RMSE**, categories flagged for retrain.  
Also note **tiered fallback** (LSTM → statistical → demographic) — show one example API response from `/api/v1/predictions/summary/all-categories`.

**C. Fraud (Isolation Forest)**

```powershell
py -3 -m backend.ml.anomaly.run_pipeline eval
```

Plus after you have real alert resolutions:

```powershell
py -3 backend\ml\production_mlflow_weekly.py
```

Record: **false positive rate** on resolved alerts (target **&lt;5%** per `update.md`), total alerts, severity distribution.

**D. RAG — three-way comparison (strong FYP table)**

```powershell
py -3 -m backend.rag.evaluation.run_ragas --config all --limit 40 --output-dir evidence\rag_eval --ragas
py -3 -m backend.rag.evaluation.compare_configs evidence\rag_eval\summary_A.json evidence\rag_eval\summary_B.json evidence\rag_eval\summary_C.json -o evidence\rag_eval\compare.md
```

Save: `compare.md` + JSON summaries. Metrics (when `--ragas` + keys set): faithfulness, context recall/overlap, latency, cost if logged.

Targets from `update.md`: faithfulness **&gt;0.85**, relevancy **&gt;0.80**, P95 **&lt;2s** (measure with Locust if possible).

**E. Multi-agent advisor**

```powershell
py -3 backend\agents\tools\test_harness.py <real_user_uuid>
```

Record: pass/fail per scenario, sample tool traces.  
Enable **LangSmith** for 5–10 real chats; export 2–3 trace screenshots (routing, RAG chunks, tools).

---

### 3. System / ops evidence (shows “production thinking”)

| What | How | Save as |
|------|-----|---------|
| API health | `GET /health` | `evidence/health.json` |
| Prometheus | `GET /metrics` while API + worker running | `evidence/prometheus.txt` |
| ETL queue | Admin endpoints in `backend/api/routers/admin.py` (staging depth, DLQ) | Screenshot + counts |
| Parser admin eval | `POST /api/admin/parser/run-evaluation` (if admin auth configured) | JSON response |
| Celery | One successful SMS ingest → staging → processed (timestamps) | Screenshot of `task_progress` or logs |
| Mobile E2E | Six providers: ingest → Home card → Edit balance → net worth updates | Screen recording |

**Locust (optional but impressive):**

```powershell
pip install -r requirements-load.txt
locust -f backend\rag\evaluation\locustfile.py --host http://YOUR_API --headless -u 10 -r 2 -t 60s
```

Save: requests/s, p95 latency, failure %.

---

### 4. Product evidence (screenshots — FYP report § “Implementation”)

Capture **real or realistic seeded data** for:

| Screen | Proves |
|--------|--------|
| Home | Net worth from 6 accounts, estimated nudge, emergency fund, health score, budget strip |
| Transactions | Category edit sticks, self-transfer toggle, GPS location-context card |
| Analytics | Donut, heatmap, LSTM predictions + “create budgets from forecast” |
| Goals | Named goal, progress %, Finni with `goal_id` |
| Investments | Holdings, events, P/L, Shariah flag |
| Zakat | Nisab, Hawl, live gold rate, advisor `screen_context: zakat` |
| Finni | SSE stream, routing pill, thumbs feedback |
| Fraud | Alert inbox, resolve false positive |
| Profile | GPS P2P toggle, Gmail/SMS setup |
| Background | SMS/notification capture (dev build) |

Also capture: **onboarding** (opening balance anchor), **bulk SMS import** completing without stuck 80% bar.

---

## One folder for your FYP / CV: `evidence/` (recommended)

```
evidence/
  pytest_results.txt
  parser_eval_full.json
  parser_eval_summary.md          # you write: 1-page table from JSON
  rag_eval/compare.md + summary_*.json
  mlflow_screenshots/               # BERT, LSTM backtest, production weekly
  langsmith_traces/                 # 3 PNGs
  prometheus.txt
  feature_screenshots/              # 15–20 PNGs
  architecture.pdf                  # export README mermaid
  METRICS_SUMMARY.md                # single page: all key numbers
```

**`METRICS_SUMMARY.md`** is what interviewers read first. Example bullets:

- **235** golden parser tests, **X%** overall pass, **Y%** on JazzCash SMS  
- **N** pytest tests passing  
- BERT weighted F1 **0.XX**, production correction rate **X%**  
- LSTM backtest MAE **X%** (top 5 categories table)  
- RAG Config C faithfulness **0.XX** vs A **0.XX**  
- Fraud FP rate **X%** on resolved alerts  
- **22** categories, **6** Home providers, **32** API routers  
- Ingestion: SMS + notifications + Gmail + OCR + manual  
- Multi-agent advisor: **8** specialist intents + RAG + LSTM tools  

---

## Features in code but weak/absent in MD (mention in report)

These are worth calling out even if not in `latest-changes.md`:

- Anchor-aware ledger + `skips_balance_ledger` (self-transfer, pre-anchor imports)
- GPS spend context (P2P at venue) + profile toggle
- Investment ledger separate from net worth
- Emergency fund on Home + goals deep link
- Spending DNA / population benchmarks (`spending_dna.py`) — may 503 until benchmarks built
- Merchant KG + user corrections + drift monitor
- Offline SMS/notification queue
- Corrective RAG + Self-RAG checkpoints
- Weekly recap, spending insights agent, categorization feedback → KG

---

## What NOT to spend time on before first deploy

- Perfect 100% parser pass on all 235 samples (report honest numbers + failure analysis)
- 8 weeks of MLflow trend (one good run + methodology is enough for FYP)
- Full Airflow DAG fleet running in prod (screenshot DAG list + one manual run is fine)
- Training BERT from scratch if artifact already exists (eval + correction rate matters more)

---

## Minimum “CV-ready” set (if time is short)

1. `pytest` green log  
2. `provider_pattern_eval.py --json` → one summary table  
3. RAG `run_ragas --config all` → `compare.md`  
4. LSTM `backtest` output  
5. `production_mlflow_weekly.py` or fraud stats from DB  
6. **12–15 app screenshots** + **1 architecture diagram**  
7. **`METRICS_SUMMARY.md`** (one page)

---

When you’ve run these, paste or attach `parser_eval_full.json` summary, pytest count, and any RAG compare table — I can help turn them into FYP report sections or CV bullet points. Tell me what you want to tackle first (parser eval, RAG, tests, or the evidence folder structure).