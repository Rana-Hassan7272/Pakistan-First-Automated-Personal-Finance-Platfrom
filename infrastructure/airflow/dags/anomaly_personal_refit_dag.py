from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "finguard-anomaly",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ensure_sys_path() -> None:
    root = _repo_root()
    backend = root / "backend"
    bp = str(backend)
    if bp not in sys.path:
        sys.path.insert(0, bp)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _refit_personal_models() -> dict[str, Any]:
    _ensure_sys_path()
    os.environ.setdefault("PYTHONPATH", str(_repo_root() / "backend"))
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.ml.anomaly import config as cfg
    from backend.ml.anomaly.jobs import ScoringJob

    supabase = get_supabase_admin_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30 * cfg.PERSONAL_MODEL_LOOKBACK_MONTHS)).isoformat()
    res = (
        supabase.table("transactions")
        .select("user_id")
        .gte("transaction_date", cutoff)
        .eq("is_fraud_flagged", False)
        .limit(8000)
        .execute()
    )
    rows = res.data or []
    from collections import Counter

    counts = Counter(str(r["user_id"]) for r in rows if r.get("user_id"))
    eligible = [uid for uid, n in counts.items() if n >= cfg.N_TX_PERSONAL_THRESHOLD]
    eligible.sort(key=lambda u: counts[u], reverse=True)
    cap = 150
    eligible = eligible[:cap]

    scorer = ScoringJob()
    ok = 0
    skipped = 0
    for uid in eligible:
        tx = (
            supabase.table("transactions")
            .select("amount_paisa, transaction_date, merchant_canonical, merchant_raw, category")
            .eq("user_id", uid)
            .eq("is_fraud_flagged", False)
            .gte("transaction_date", cutoff)
            .order("transaction_date", desc=False)
            .limit(2000)
            .execute()
        )
        trows = tx.data or []
        if len(trows) < cfg.N_TX_PERSONAL_THRESHOLD:
            skipped += 1
            continue
        recs = []
        for r in trows:
            ap = r.get("amount_paisa")
            if ap is None:
                continue
            recs.append(
                {
                    "user_id": uid,
                    "amount": float(ap) / 100.0,
                    "timestamp": r.get("transaction_date"),
                    "merchant": (r.get("merchant_canonical") or r.get("merchant_raw") or "").strip(),
                    "category": (r.get("category") or "").strip(),
                }
            )
        if len(recs) < cfg.N_TX_PERSONAL_THRESHOLD:
            skipped += 1
            continue
        hist = pd.DataFrame.from_records(recs)
        try:
            bundle = scorer.refit_personal_model(uid, hist, mlflow_log=True)
            if bundle is not None:
                ok += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.warning("personal refit failed user=%s: %s", uid[:8], exc)
            skipped += 1

    logger.info("anomaly personal refit: eligible=%d fitted=%d skipped=%d", len(eligible), ok, skipped)
    return {"eligible_users": len(eligible), "fitted": ok, "skipped": skipped}


with DAG(
    dag_id="anomaly_personal_refit_dag",
    default_args=_DEFAULT_ARGS,
    description="Refit per-user Isolation Forest fraud models from recent normal transactions (Phase 11: cron 3am)",
    schedule_interval="0 3 1 * *",  # Phase 11: Changed from 0 6 1 * * to 0 3 1 * *
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["anomaly", "fraud", "ml", "phase11"],
) as dag_refit:
    PythonOperator(
        task_id="refit_personal_isolation_forest",
        python_callable=_refit_personal_models,
    )
