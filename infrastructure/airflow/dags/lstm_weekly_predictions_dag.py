from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "finguard-lstm",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _run_lstm_cli(subcommand: str) -> None:
    root = _repo_root()
    backend = root / "backend"
    env = dict(os.environ)
    sep = os.pathsep
    bp = str(backend)
    env["PYTHONPATH"] = f"{bp}{sep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else bp
    script = backend / "ml" / "lstm" / "run_pipeline.py"
    cmd = [sys.executable, str(script), subcommand]
    logger.info("lstm_cli %s", cmd)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


def _has_sufficient_history(sb, user_id: str, min_months: int = 3) -> bool:
    """Phase 11: Check if user has at least N months of transaction history."""
    from datetime import datetime, timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=min_months * 30)).isoformat()

    try:
        resp = (
            sb.table("transactions")
            .select("transaction_id", count="exact")
            .eq("user_id", user_id)
            .gte("transaction_date", cutoff)
            .limit(1)
            .execute()
        )
        # Need at least some transactions in the period
        return (resp.count or 0) > 0
    except Exception as exc:
        logger.warning(f"Could not check history for {user_id}: {exc}")
        return True  # Default to allowing if check fails


def _weekly_predict() -> None:
    from backend.api.core.supabase_client import get_supabase_admin_client

    sb = get_supabase_admin_client()
    users = sb.table("users").select("user_id").execute().data or []

    skipped_users = []
    eligible_users = []

    for user in users:
        uid = user["user_id"]
        if _has_sufficient_history(sb, uid, min_months=3):
            eligible_users.append(uid)
        else:
            skipped_users.append(uid)
            logger.info(f"Skipping LSTM prediction for {uid}: insufficient history (< 3 months)")

    logger.info(f"LSTM predictions: {len(eligible_users)} eligible, {len(skipped_users)} skipped")

    # Only run predictions if there are eligible users
    if eligible_users:
        _run_lstm_cli("predict")
    else:
        logger.info("No users with sufficient history - skipping LSTM prediction run")


with DAG(
    dag_id="lstm_weekly_predictions_dag",
    default_args=_DEFAULT_ARGS,
    description="Sunday LSTM prediction job with 3mo history gate (Phase 11: cron midnight)",
    schedule_interval="0 0 * * 0",  # Phase 11: Changed from 0 7 * * 0 to 0 0 * * 0
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lstm", "forecasting", "phase11"],
) as dag_weekly:
    PythonOperator(
        task_id="run_sunday_lstm_prediction",
        python_callable=_weekly_predict,
    )
