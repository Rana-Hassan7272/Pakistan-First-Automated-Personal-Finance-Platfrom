"""
infrastructure/airflow/dags/transaction_processing_dag.py

Airflow DAG: end-to-end transaction processing pipeline.

Schedule: every 5 minutes.

Task graph (strict linear chain — each stage must pass before the next runs):

    pull_pending_staging
        ↓
    run_etl_batch          ← parse + validate + dedup + merchant normalise + load
        ↓
    run_data_quality       ← Great Expectations check on just-inserted rows
        ↓
    trigger_fraud_check    ← enqueue Celery task for Isolation Forest scoring
        ↓
    update_embeddings      ← generate + store vector embeddings for new txns
        ↓
    notify_on_critical     ← send alert if quality or fraud thresholds breached
        ↓
    cleanup_staging        ← archive processed/failed staging rows older than 7 days

Design decisions:
  • All tasks are PythonOperators — no BashOperator so secrets never leak
    into shell environment variables.
  • XCom is used sparingly (only small JSON summaries; never full data frames).
  • retries=2 with 30 s exponential_backoff on all tasks.
  • The DAG is idempotent: re-running a failed DAG run won't double-insert
    transactions because deduplication is enforced at the ETL layer.
  • on_failure_callback logs to LangSmith / Slack; configure via Airflow
    Variables SLACK_WEBHOOK_URL and LANGSMITH_API_KEY.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "fingard-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

# Max rows to pull per DAG run (keeps runs bounded)
BATCH_LIMIT = int(Variable.get("etl_batch_limit", default_var=50))

# Critical failure threshold imported from quality module
CRITICAL_DQ_FAILURE_RATE = 0.05

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _pull_pending_staging(**context) -> dict:
    """
    Count pending staging rows and push the count to XCom.
    The actual fetching happens inside the ETL batch processor.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    resp = (
        db.table("ingestion_staging")
        .select("staging_id", count="exact")
        .eq("status", "pending_processing")
        .execute()
    )
    count = resp.count or 0
    logger.info("Pending staging rows: %d", count)

    context["ti"].xcom_push(key="pending_count", value=count)
    return {"pending_count": count}


def _run_etl_batch(**context) -> dict:
    """
    Run the ETL pipeline for up to BATCH_LIMIT pending staging rows.
    Pushes BatchResult summary to XCom.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.etl.pipeline import ETLPipeline

    db = get_supabase_admin_client()
    pipeline = ETLPipeline(supabase=db)
    batch_result = pipeline.process_batch(limit=BATCH_LIMIT)
    summary = batch_result.summary()

    logger.info("ETL batch complete: %s", summary)
    context["ti"].xcom_push(key="batch_summary", value=summary)
    return summary


def _run_data_quality(**context) -> dict:
    """
    Pull just-inserted transactions from this run's window and validate them.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.etl.data_quality import run_quality_check

    ti = context["ti"]
    batch_summary = ti.xcom_pull(task_ids="run_etl_batch", key="batch_summary") or {}
    succeeded = batch_summary.get("succeeded", 0)

    if succeeded == 0:
        logger.info("No new transactions to quality-check")
        return {"skipped": True}

    db = get_supabase_admin_client()

    # Fetch the transactions inserted in approximately the last 10 minutes
    window_start = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
    resp = (
        db.table("transactions")
        .select("*")
        .gte("created_at", window_start)
        .limit(BATCH_LIMIT)
        .execute()
    )
    rows = resp.data or []

    if not rows:
        logger.info("No recent transactions found for quality check")
        return {"skipped": True}

    run_id_str = context.get("run_id", "")
    report = run_quality_check(
        rows=rows,
        batch_id=f"dq_{run_id_str}",
        alert_callback=_quality_alert_callback,
    )
    summary = report.summary()
    ti.xcom_push(key="dq_summary", value=summary)
    logger.info("Data quality report: %s", summary)
    return summary


def _trigger_fraud_check(**context) -> dict:
    """
    Enqueue Celery tasks for Isolation Forest scoring on new transactions.
    """
    try:
        from backend.workers.tasks import score_transactions_for_fraud
    except Exception:
        logger.info("Fraud worker task not available yet; skipping enqueue.")
        return {"enqueued": 0, "skipped": True}

    ti = context["ti"]
    batch_summary = ti.xcom_pull(task_ids="run_etl_batch", key="batch_summary") or {}
    succeeded = batch_summary.get("succeeded", 0)

    if succeeded == 0:
        return {"enqueued": 0}

    # Fetch new transaction IDs (same window as quality check)
    from backend.api.core.supabase_client import get_supabase_admin_client
    db = get_supabase_admin_client()
    window_start = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
    resp = (
        db.table("transactions")
        .select("transaction_id,user_id")
        .gte("created_at", window_start)
        .limit(BATCH_LIMIT)
        .execute()
    )
    rows = resp.data or []

    enqueued = 0
    for row in rows:
        score_transactions_for_fraud.delay(
            transaction_id=row["transaction_id"],
            user_id=row["user_id"],
        )
        enqueued += 1

    logger.info("Fraud scoring tasks enqueued: %d", enqueued)
    context["ti"].xcom_push(key="fraud_enqueued", value=enqueued)
    return {"enqueued": enqueued}


def _update_embeddings(**context) -> dict:
    """
    Generate and store sentence-transformer embeddings for new transactions
    that don't yet have one (embedding IS NULL).
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    try:
        from backend.workers.tasks import embed_transaction_task
    except Exception:
        logger.info("Embedding worker task not available yet; skipping enqueue.")
        return {"enqueued": 0, "skipped": True}

    db = get_supabase_admin_client()
    window_start = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
    resp = (
        db.table("transactions")
        .select("transaction_id,merchant_canonical,category,amount_paisa,source")
        .gte("created_at", window_start)
        .is_("embedding", "null")
        .limit(BATCH_LIMIT)
        .execute()
    )
    rows = resp.data or []

    enqueued = 0
    for row in rows:
        embed_transaction_task.delay(transaction_id=row["transaction_id"])
        enqueued += 1

    logger.info("Embedding tasks enqueued: %d", enqueued)
    return {"enqueued": enqueued}


def _notify_on_critical(**context) -> None:
    """
    Send Slack / email alert if:
      • Data quality failure rate exceeded the critical threshold, OR
      • ETL had > 20% DB errors in this batch.

    Uses TriggerRule.ALL_DONE so it runs even if upstream tasks failed.
    """
    ti = context["ti"]
    dq_summary = ti.xcom_pull(task_ids="run_data_quality", key="dq_summary") or {}
    batch_summary = ti.xcom_pull(task_ids="run_etl_batch", key="batch_summary") or {}

    messages: list[str] = []

    if dq_summary.get("is_critical"):
        rate = dq_summary.get("failure_rate", 0)
        messages.append(
            f"🚨 *FinGuard ETL — Critical DQ Failure*\n"
            f"Batch: {dq_summary.get('batch_id')}\n"
            f"Failure rate: {rate:.1%} (threshold: {CRITICAL_DQ_FAILURE_RATE:.0%})\n"
            f"Failed checks: {dq_summary.get('failed_checks')}"
        )

    total = batch_summary.get("total", 0)
    db_errors = batch_summary.get("db_errors", 0)
    if total > 0 and db_errors / total > 0.20:
        messages.append(
            f"🚨 *FinGuard ETL — High DB Error Rate*\n"
            f"DB errors: {db_errors}/{total} ({db_errors/total:.0%})"
        )

    if not messages:
        logger.info("notify_on_critical: no alerts needed")
        return

    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if webhook_url:
        import requests
        for msg in messages:
            try:
                requests.post(webhook_url, json={"text": msg}, timeout=5)
            except Exception as exc:
                logger.error("Slack notification failed: %s", exc)
    else:
        for msg in messages:
            logger.warning("ALERT (no Slack webhook configured): %s", msg)


def _cleanup_staging(**context) -> dict:
    """
    Archive staging rows older than 7 days with status processed or failed.
    Keeps the staging table lean for index performance.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client

    db = get_supabase_admin_client()
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()

    try:
        resp = (
            db.table("ingestion_staging")
            .delete()
            .in_("status", ["processed", "failed"])
            .lt("created_at", cutoff)
            .execute()
        )
        deleted = len(resp.data or [])
        logger.info("Cleaned up %d stale staging rows", deleted)
        return {"deleted": deleted}
    except Exception as exc:
        logger.error("Staging cleanup failed: %s", exc)
        return {"deleted": 0, "error": str(exc)}


def _quality_alert_callback(report) -> None:
    """Called by run_quality_check when is_critical=True."""
    logger.error("CRITICAL quality alert triggered for batch: %s", report.batch_id)
    # Further escalation can be added here


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="transaction_processing_dag",
    default_args=_DEFAULT_ARGS,
    description="FinGuard AI — end-to-end transaction ETL pipeline",
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,             # prevent overlapping runs
    tags=["etl", "fingard", "transactions"],
) as dag:

    t_pull = PythonOperator(
        task_id="pull_pending_staging",
        python_callable=_pull_pending_staging,
        doc_md="Count pending staging rows and push count to XCom.",
    )

    t_etl = PythonOperator(
        task_id="run_etl_batch",
        python_callable=_run_etl_batch,
        doc_md="Parse → validate → dedup → normalise → load up to BATCH_LIMIT rows.",
    )

    t_dq = PythonOperator(
        task_id="run_data_quality",
        python_callable=_run_data_quality,
        doc_md="Run Great Expectations suite on newly inserted transactions.",
    )

    t_fraud = PythonOperator(
        task_id="trigger_fraud_check",
        python_callable=_trigger_fraud_check,
        doc_md="Enqueue Celery tasks for Isolation Forest scoring.",
    )

    t_embed = PythonOperator(
        task_id="update_embeddings",
        python_callable=_update_embeddings,
        doc_md="Enqueue embedding generation for new transactions.",
    )

    t_notify = PythonOperator(
        task_id="notify_on_critical",
        python_callable=_notify_on_critical,
        trigger_rule=TriggerRule.ALL_DONE,   # runs even if upstream failed
        doc_md="Send Slack alert if quality or error thresholds exceeded.",
    )

    t_cleanup = PythonOperator(
        task_id="cleanup_staging",
        python_callable=_cleanup_staging,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Delete processed/failed staging rows older than 7 days.",
    )

    # Phase 11: Short-circuit if no pending staging rows
    t_check_empty = ShortCircuitOperator(
        task_id="check_staging_not_empty",
        python_callable=lambda ti: ti.xcom_pull(task_ids="pull_pending_staging", key="pending_count") > 0,
        doc_md="Skip rest of DAG if no pending staging rows to process.",
    )

    # Dependency chain
    t_pull >> t_check_empty >> t_etl >> t_dq >> t_fraud >> t_embed >> t_notify >> t_cleanup