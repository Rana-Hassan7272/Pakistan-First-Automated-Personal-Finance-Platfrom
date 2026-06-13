"""
infrastructure/airflow/dags/retrieval_memory_decay_dag.py

Phase 11: Retrieval memory decay DAG.

Schedule: Weekly on Monday at 4am (0 4 * * 1)
Logic:
- DELETE FROM retrieval_memory WHERE last_used < NOW() - INTERVAL '30 days'
- Log deletion count to MLflow
- This keeps vector DB lean and query performance fast
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from dotenv import load_dotenv
load_dotenv(os.path.join(_repo_root, ".env"))

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "finguard",
    "retries": 1,
    "email_on_failure": False,
}

# Phase 11: Memory retention period - delete unused memories older than 30 days
RETENTION_DAYS = 30


def cleanup_retrieval_memory(**context):
    """
    Phase 11: Delete old retrieval_memory entries and log to MLflow.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    import mlflow

    sb = get_supabase_admin_client()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()

    try:
        # Count before deletion for logging
        count_resp = (
            sb.table("retrieval_memory")
            .select("id", count="exact")
            .lt("last_used", cutoff)
            .execute()
        )
        to_delete = count_resp.count or 0

        if to_delete == 0:
            logger.info("No old retrieval_memory entries to delete")
            result = {"deleted": 0, "retention_days": RETENTION_DAYS}
        else:
            # Perform deletion
            sb.table("retrieval_memory").delete().lt("last_used", cutoff).execute()
            logger.info(f"Deleted {to_delete} old retrieval_memory entries")
            result = {"deleted": to_delete, "retention_days": RETENTION_DAYS}

        # Log to MLflow
        try:
            mlflow.set_experiment("retrieval_memory_maintenance")
            with mlflow.start_run(run_name="weekly_decay"):
                mlflow.log_param("retention_days", RETENTION_DAYS)
                mlflow.log_metric("deleted_count", result["deleted"])
                mlflow.log_metric("timestamp", datetime.now(timezone.utc).timestamp())
        except Exception as exc:
            logger.warning(f"MLflow logging failed: {exc}")

        return result

    except Exception as exc:
        logger.error(f"Failed to cleanup retrieval_memory: {exc}")
        raise


with DAG(
    dag_id="retrieval_memory_decay_dag",
    description="Weekly cleanup of stale retrieval_memory (>30 days unused) - Monday 4am (Phase 11)",
    schedule_interval="0 4 * * 1",  # Monday at 4am
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "rag", "memory", "maintenance", "phase11"],
) as dag:
    PythonOperator(
        task_id="cleanup_retrieval_memory",
        python_callable=cleanup_retrieval_memory,
        doc_md=f"Delete retrieval_memory entries where last_used > {RETENTION_DAYS} days ago.",
    )
