# FinGuard MLflow metrics (evidence snapshot)

_Source: MLflow UI 2.16.2, `backend/ml/mlruns`. Logged on training laptop; copy for FYP/CV benchmarks._

---

## 1. BERT transaction categorization

| Field | Value |
|-------|--------|
| Experiment | `bert_transaction_categorization` |
| Run name | `sassy-colt-6` |
| Run ID | `7879ff6ee0194a728aceb84af19f866f` |
| Status | Finished |
| Duration | 2.7 min |
| Source | `train.py` |
| Registered model | `bert_transaction_classifier` **v2** |

### Training parameters

| Parameter | Value |
|-----------|--------|
| model | bert-base-multilingual-cased |
| epochs | 5 |
| batch_size | 16 |
| lr | 2e-05 |
| weight_decay | 0.01 |
| warmup_ratio | 0.1 |
| grad_accum | 2 |
| label_smoothing | 0.1 |
| class_weighted_loss | True |
| weighted_sampler | True |
| early_stopping_patience | 2 |
| best_epoch | 4 |
| best_val_weighted_f1 | 0.8874 |

### Metrics

| Metric | Value |
|--------|--------|
| train_loss | 0.7935 |
| val_accuracy | **0.8926** |
| val_macro_f1 | **0.8577** |
| val_weighted_f1 | **0.8874** |

**Report line:** BERT multilingual cased — weighted F1 **88.7%**, macro F1 **85.8%**, accuracy **89.3%** on validation (epoch 4 best).

---

## 2. LSTM — training (all categories)

| Field | Value |
|-------|--------|
| Experiment | (LSTM train run) |
| Run ID | `cd07830dce5e4a51bfb0ca92d0733aee` |
| Created | 2026-05-10 15:45:51 |
| Duration | 57.9 s |
| Source | `run_pipeline.py` |

### Parameters

- **categories:** 21 expense categories (food_dining, grocery, utilities, mobile_internet, transport_fuel, travel, entertainment, health_fitness, social_events, shopping_general, clothing_footwear, electronics_gadgets, healthcare_pharmacy, education, cash_withdrawal, transfers, income_salary, bills_government, investments, charity_donations, subscriptions)

### Metrics

| Metric | Value |
|--------|--------|
| test/mean_mae_all_categories | **0.2637** |
| test/mean_rmse_all_categories | **0.4069** |

_Note: normalized scale on synthetic/holdout test split, not PKR._

---

## 3. LSTM — inference benchmark (MC dropout)

| Field | Value |
|-------|--------|
| Run ID | `0aee8f4e52924f6fb14be373713944f1` |
| Created | 2026-05-10 22:00:31 |
| Duration | 18.0 s |
| Source | `run_pipeline.py` |

### Parameters

| Parameter | Value |
|-----------|--------|
| input_seq_len | 12 |
| pred_horizon | 3 months |
| mc_samples | 100 |
| n_users | 100 |
| categories | 21 (same list as above) |

### Key metrics

| Metric | Value |
|--------|--------|
| lstm_inference_n_sequences | 100 |
| lstm_inference_total_ms | **12,757.9** (~128 ms/sequence) |

Per-category **mean** and **p10/p90** for months 1–3 logged (191 metrics). Examples:

| Category | mean_month_1 (approx.) | mean_month_3 (approx.) |
|----------|------------------------|-------------------------|
| cash_withdrawal | 17,207 | 17,478 |
| bills_government | 4,745 | 4,758 |

_Full per-category series in MLflow run `0aee8f4e52924f6fb14be373713944f1`._

---

## 4. LSTM — monthly backtest (2026-04)

| Field | Value |
|-------|--------|
| Run ID | `07a7072e7f214772a32e01dfaaefa5f6` |
| Created | 2026-05-10 15:53:48 |
| Duration | 309 ms |
| Source | `run_pipeline.py` |

### Parameters

| Parameter | Value |
|-----------|--------|
| backtest_month | 2026-04-01 |
| has_stored_predictions | True |
| retrain_mae_threshold_pct | 0.25 (25%) |
| retrain_recommended | **True** |
| n_categories_needing_retrain | **21** (all evaluated categories) |

### Sample per-category backtest (MAE % of actual)

| Category | mae_pct | retrain_flag |
|----------|---------|--------------|
| bills_government | 46.3% | 1 |
| cash_withdrawal | 47.9% | 1 |
| charity_donations | 52.8% | 1 |
| clothing_footwear | 46.6% | 1 |

_All 21 categories flagged for retrain at 25% MAE threshold on this synthetic backtest month._

**Report line:** Backtest run recommends retrain when MAE% > 25%; on 2026-04 synthetic holdout, all 21 categories exceeded threshold (typical for synthetic vs production tuning).

---

## 5. Isolation Forest — fraud detection (global test)

| Field | Value |
|-------|--------|
| Experiment | (anomaly / fraud) |
| Run ID | `27a296e79f194c7b80892210df2d1cab` |
| Created | 2026-05-10 22:54:27 |
| Duration | 94 ms |
| Source | `backend/ml/anomaly/run_pipeline.py` |
| Tags | scope=global, score_source=fusion_probability, split=test |

### Metrics

| Metric | Value |
|--------|--------|
| n_samples | **35,486** |
| fraud_count_true | 684 |
| fraud_count_pred | 797 |
| precision | **0.8243** |
| recall | **0.9605** |
| f1 | **0.8872** |
| average_precision | **0.9513** |
| threshold | 0.95 |

**Report line:** Global Isolation Forest test — F1 **88.7%**, precision **82.4%**, recall **96.1%**, AP **95.1%** (threshold 0.95).

---

## 6. LSTM backtest — local re-run (2026-06-01)

| Field | Value |
|-------|--------|
| Source | `python -m backend.ml.lstm.run_pipeline backtest` |
| Artifact | `evidence/lstm_backtest.txt` |
| Retrain recommended | Yes — **charity_donations only** (MAE **26.67%**) |
| Other categories | MAE **6.2%–18.0%** (under 25% threshold) |

**Report line:** 20/21 categories within 25% MAE on latest synthetic month; only charity_donations flagged for retrain.

---

## 7. Fraud eval — local re-run (2026-06-01)

| Metric | Value |
|--------|--------|
| Artifact | `evidence/fraud_eval.txt` |
| Precision | **0.8243** (target > 0.80) |
| Recall | **0.9605** (target > 0.75) |
| F1 | **0.8872** |
| AUPRC | **0.9513** |
| Gate | **PASS** |

---

## 8. Production snapshot — Supabase live (2026-06-01)

| Field | Value |
|-------|--------|
| Run name | `production_weekly_20260601_17` |
| Run ID | `99c029a155df423490d5f440ef0278a0` |
| Experiment | `finguard_production_metrics` |
| supabase_connected | True |

### BERT / fraud (production DB)

| Metric | Value | Note |
|--------|--------|------|
| bert_feedback_resolved_with_flag | 0 | No resolved BERT feedback rows yet |
| bert_category_correction_count | 0 | — |
| fraud_alerts_total | 0 | No alerts in DB |
| fraud_resolved_fp_count | 0 | — |
| fraud_resolved_cf_count | 0 | FP rate N/A until users resolve alerts |

### Multi-agent (agent_run_logs)

| Metric | Value |
|--------|--------|
| agent_total_runs | 5 |
| agent_success_rate | **1.0** |
| agent_retry_rate | 0 |

| Agent | runs | success | avg latency (ms) | avg LLM calls | avg tool calls |
|-------|------|---------|------------------|---------------|----------------|
| expense_analysis | 2 | 1.0 | 35,027 | 1 | 5 |
| investment_advice | 2 | 1.0 | 27,835 | 1 | 8 |

**Report line:** Advisor tools run successfully in prod logs; fraud/BERT correction KPIs need more real usage data.

---

## 9. BERT inference latency (local CPU, 2026-06-01)

| Metric | ms |
|--------|-----|
| p50 | **99.87** |
| p95 | **270.0** |
| p99 | 5015.4 (first-load warmup skew) |
| mean | 302.02 |
| n | 50 |

_Artifact: `evidence/bert_inference_latency.txt`. Steady-state forward pass ~80–270 ms after model warm-up._

---

## 10. Advisor API latency smoke (local, 2026-06-01)

| Metric | ms |
|--------|-----|
| n | 10/10 HTTP 200 |
| p50 | **2013.86** |
| p95 | **25436.35** |
| p99 | **40700.01** |
| mean | 6266.55 |

_Command: `python evaluation/advisor_latency_smoke.py -n 10` — artifact `evidence/advisor_latency_smoke.txt`. Full SSE stream to `/api/v1/advisor/chat` (rag-ish message). Tail latencies include cold start / full RAG+agent path; compare with LangSmith production traces._

---

## 11. LangSmith — advisor latency (multi-agent)

| Metric | Value |
|--------|--------|
| Project | `finguard-agents` |
| p50 | **~4 s** |
| p99 | **47.69 s** |

_Source: LangSmith UI; see `evidence/langsmith_advisor_latency.txt`._

---

## 12. Agent tool harness (2026-06-01)

| Field | Value |
|-------|--------|
| User ID | `77f69279-173c-4286-8f27-7f45d8d55f34` |
| Artifact | `evidence/agent_harness.txt` (final run after `rank_bm25` install) |
| Scenarios passed | **5 / 5** |
| Tool calls | 25 |
| Avg latency | **749 ms** |
| Failed tool calls | 3 (non-blocking) |
| Verdict | **All scenarios pass. Ready to build agents.** |

| Scenario | Result |
|----------|--------|
| Expense Analysis Flow | **PASS** |
| Investment Advice Flow | **PASS** — `rag halal investment` **chunks=3** |
| Zakat Calculation Flow | **PASS** — RAG confidence **0.735** |
| Fraud Query Flow | **PASS** |
| Budget Advice Flow | **PASS** — LSTM **21** categories |

---

## 10. Evidence file index (session)

| Step | Artifact | Status |
|------|----------|--------|
| pytest | `evidence/pytest_results.txt` | 304 passed, 1 failed |
| parser eval | `evidence/parser_eval_full.json` | 214/235 (91.1%) |
| RAG KB verify | `evidence/rag_kb_verify.txt` | 102 docs, 69k chunks |
| RAG A/B/C | `evidence/rag_eval/compare.md` | cosine 0.69–0.72 |
| LSTM backtest | `evidence/lstm_backtest.txt` | 20/21 categories OK |
| Fraud eval | `evidence/fraud_eval.txt` | Gate PASS |
| Production KPIs | MLflow `production_weekly_20260601_17` | Logged |
| Agent harness | `evidence/agent_harness.txt` | **5/5** scenarios |

---

## Quick CV bullets

- **BERT (train):** val weighted F1 **0.887**, macro F1 **0.858**, accuracy **0.893**
- **LSTM (local backtest):** 20/21 categories MAE &lt; 25%; charity_donations **26.7%** → retrain
- **Fraud ML (offline):** F1 **0.887**, AP **0.951** on 35k test transactions
- **RAG KB:** 102 documents, **69,490** child chunks; Zakat RAG harness confidence **0.735**
- **Agents:** harness **4/5**; production agent success rate **100%** (5 runs)
