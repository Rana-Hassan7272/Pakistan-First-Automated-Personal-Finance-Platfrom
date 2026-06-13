from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "finguard-bert",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _backend_env(root: Path) -> dict:
    backend = root / "backend"
    env = dict(os.environ)
    sep = os.pathsep
    bp = str(backend)
    env["PYTHONPATH"] = f"{bp}{sep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else bp
    return env


def _check_corrections_threshold(**context) -> bool:
    """
    Phase 11: Only retrain if we have 50+ user corrections since last run.
    Returns True if threshold met, False to short-circuit (skip) the DAG.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    import mlflow

    sb = get_supabase_admin_client()

    # Count corrections in last 7 days
    from datetime import datetime, timezone, timedelta
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    try:
        resp = (
            sb.table("categorization_feedback")
            .select("id", count="exact")
            .gte("created_at", week_ago)
            .execute()
        )
        correction_count = resp.count or 0
    except Exception as exc:
        logger.warning(f"Could not count corrections: {exc}")
        correction_count = 0

    THRESHOLD = 50
    should_run = correction_count >= THRESHOLD

    # Log to MLflow whether we skip or run
    try:
        mlflow.set_experiment("bert_retrain_decisions")
        with mlflow.start_run(run_name="weekly_gate_check"):
            mlflow.log_param("correction_count", correction_count)
            mlflow.log_param("threshold", THRESHOLD)
            mlflow.log_param("should_run", should_run)
            if not should_run:
                mlflow.log_metric("skipped", 1)
                logger.info(f"Skipping BERT retrain: only {correction_count} corrections (< {THRESHOLD})")
            else:
                mlflow.log_metric("skipped", 0)
                logger.info(f"Proceeding with BERT retrain: {correction_count} corrections (>= {THRESHOLD})")
    except Exception as exc:
        logger.warning(f"MLflow logging failed: {exc}")

    return should_run


def _run_bert_full_pipeline() -> None:
    root = _repo_root()
    env = _backend_env(root)
    script = root / "backend" / "ml" / "bert" / "run_pipeline.py"
    cmd = [sys.executable, str(script)]
    logger.info("bert_retrain %s", cmd)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


def _run_bert_promotion() -> None:
    root = _repo_root()
    env = _backend_env(root)
    cmd = [sys.executable, "-m", "ml.bert.promotion"]
    logger.info("bert_promotion %s", cmd)
    subprocess.run(cmd, cwd=str(root), env=env, check=True)


with DAG(
    dag_id="bert_weekly_retrain_dag",
    default_args=_DEFAULT_ARGS,
    description="Weekly BERT retrain with 50+ corrections gate (Phase 11: cron 2am + ShortCircuit)",
    schedule_interval="0 2 * * 0",  # Phase 11: Changed from 0 3 * * 0 to 0 2 * * 0
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["bert", "ml", "phase8", "phase11"],
) as dag_bert:
    # Phase 11: Short-circuit if < 50 corrections
    t_check_threshold = ShortCircuitOperator(
        task_id="check_corrections_threshold",
        python_callable=_check_corrections_threshold,
        doc_md="Skip retraining if fewer than 50 user corrections in last 7 days.",
    )

    t_train = PythonOperator(
        task_id="bert_run_pipeline_train",
        python_callable=_run_bert_full_pipeline,
    )
    t_promote = PythonOperator(
        task_id="bert_promotion_gate",
        python_callable=_run_bert_promotion,
    )
    t_check_threshold >> t_train >> t_promote
