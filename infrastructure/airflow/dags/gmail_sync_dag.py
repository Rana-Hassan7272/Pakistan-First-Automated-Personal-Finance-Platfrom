"""
infrastructure/airflow/dags/gmail_sync_dag.py

Phase 12 Step 2: Automated Gmail sync + auto-ETL chain DAG.

Schedule: Every 24 hours (prefer Celery Beat etl.dispatch_scheduled_gmail_sync if Airflow is off)
Logic:
- Query gmail_connections (not user_gmail_tokens — that table is gone)
- Per user: skip if synced within last 2 hours
- First ever sync (first_sync_completed=False) → 90-day window
- Subsequent syncs → salary_day window
- After gmail_sync_task succeeds → auto-chains ETL (already wired in gmail_tasks.py)
"""

from __future__ import annotations

import calendar
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SYNC_INTERVAL_HOURS = 24


def _should_sync_user(last_synced_at: str | None) -> bool:
    if not last_synced_at:
        return True
    try:
        last_sync = datetime.fromisoformat(last_synced_at.replace("Z", "+00:00"))
        return last_sync < datetime.now(timezone.utc) - timedelta(hours=SYNC_INTERVAL_HOURS)
    except Exception:
        return True


def _compute_sync_from_date(salary_day: int, first_sync_completed: bool) -> str:
    """Return Gmail query date string (YYYY/MM/DD)."""
    now = datetime.now(timezone.utc)
    if not first_sync_completed:
        return (now - timedelta(days=90)).strftime("%Y/%m/%d")
    salary_day = max(1, min(salary_day, 31))
    days_in_curr = calendar.monthrange(now.year, now.month)[1]
    effective_day_curr = min(salary_day, days_in_curr)
    if now.day >= effective_day_curr:
        sync_from = now.replace(day=effective_day_curr, hour=0, minute=0, second=0, microsecond=0)
    else:
        last_month = now.month - 1 if now.month > 1 else 12
        last_month_year = now.year if now.month > 1 else now.year - 1
        days_in_last = calendar.monthrange(last_month_year, last_month)[1]
        sync_from = now.replace(
            year=last_month_year, month=last_month,
            day=min(salary_day, days_in_last),
            hour=0, minute=0, second=0, microsecond=0,
        )
    return sync_from.strftime("%Y/%m/%d")


def dispatch_gmail_sync_for_all_users(**context):
    """
    Phase 12 Step 2: Dispatch Gmail sync Celery tasks for users needing sync.
    Uses gmail_connections table. Handles first-sync 90-day window.
    ETL is chained automatically inside gmail_sync_task on success.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.api.routers.tasks import create_task_progress
    from backend.etl.tasks.gmail_tasks import gmail_sync_task

    sb = get_supabase_admin_client()

    # Fetch all active Gmail connections with first_sync_completed flag
    connections_resp = (
        sb.table("gmail_connections")
        .select("user_id, access_token, refresh_token, token_type, scope, raw_token_json, "
                "last_synced_message_id, last_synced_at, first_sync_completed")
        .eq("is_active", True)
        .execute()
    )
    connections = connections_resp.data or []

    dispatched = 0
    skipped = 0
    failed = 0

    for row in connections:
        uid = row["user_id"]
        last_synced = row.get("last_synced_at")

        if not _should_sync_user(last_synced):
            logger.info("Skipping %s: synced within last %dh", uid[:8], SYNC_INTERVAL_HOURS)
            skipped += 1
            continue

        try:
            # Get salary_day for this user
            user_resp = sb.table("users").select("salary_day").eq("user_id", uid).limit(1).execute()
            salary_day = int((user_resp.data or [{}])[0].get("salary_day") or 1)
            first_sync_completed = bool(row.get("first_sync_completed", False))

            sync_from_date = _compute_sync_from_date(salary_day, first_sync_completed)

            # Build OAuth token dict
            raw = row.get("raw_token_json") or {}
            oauth_token = {
                "token": row["access_token"],
                "refresh_token": row.get("refresh_token") or raw.get("provider_refresh_token") or raw.get("refresh_token"),
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": os.environ.get("GOOGLE_OAUTH_CLIENT_ID"),
                "client_secret": os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET"),
                "scopes": [s for s in (row.get("scope") or "").split() if s],
            }

            task_id = create_task_progress(
                supabase=sb,
                user_id=uid,
                task_type="gmail_sync",
                total=0,
                estimated_seconds=60,
            )

            gmail_sync_task.apply_async(
                kwargs={
                    "user_id": uid,
                    "task_id": task_id,
                    "oauth_token": oauth_token,
                    "last_synced_message_id": row.get("last_synced_message_id"),
                    "sync_from_date": sync_from_date,
                    "first_sync_completed": first_sync_completed,
                },
                countdown=0,
            )

            logger.info(
                "Dispatched Gmail sync user=%s task=%s from=%s first_sync_done=%s",
                uid[:8], task_id, sync_from_date, first_sync_completed,
            )
            dispatched += 1

        except Exception as exc:
            logger.error("Failed to dispatch Gmail sync for %s: %s", uid[:8], exc)
            failed += 1

    result = {
        "dispatched": dispatched,
        "skipped": skipped,
        "failed": failed,
        "sync_interval_hours": SYNC_INTERVAL_HOURS,
    }
    logger.info("Gmail sync DAG complete: %s", result)
    return result


with DAG(
    dag_id="gmail_sync_dag",
    description="Phase 12 Step 2: Every-2h Gmail sync + auto-ETL chain",
    schedule_interval="0 3 * * *",  # Daily 03:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "gmail", "sync", "phase12"],
) as dag:
    PythonOperator(
        task_id="dispatch_gmail_sync_tasks",
        python_callable=dispatch_gmail_sync_for_all_users,
        doc_md="Per-user: skip if synced <2h ago. First sync → 90 days. Subsequent → salary_day. ETL auto-chains.",
    )
