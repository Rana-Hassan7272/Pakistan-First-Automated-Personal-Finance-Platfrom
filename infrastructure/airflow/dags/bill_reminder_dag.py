"""
Bill reminder push notifications (Phase 14) and missing bill detection (Phase 16).

- Reminds users when bills are due in 5 days and again in 2 days.
- Detects recurring payments that have not arrived by expected date + grace.
Schedule: Twice daily at 8am and 6pm.
"""

from __future__ import annotations

import logging
import os
import sys
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

REMINDER_DAYS = (3, 2)


def _days_until_due(due_date_str: str, now: datetime) -> int | None:
    try:
        due = datetime.fromisoformat(str(due_date_str).replace("Z", "+00:00"))
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        delta = (due.date() - now.date()).days
        return delta
    except Exception:
        return None


def send_bill_reminders(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.api.services.expo_push import send_bill_reminder_push

    sb = get_supabase_admin_client()
    now = datetime.now(timezone.utc)

    try:
        bills_resp = (
            sb.table("bill_reminders")
            .select("bill_id,user_id,bill_name,amount_paisa,due_date,is_active")
            .eq("is_active", True)
            .execute()
        )
    except Exception as exc:
        logger.error("Could not load bill_reminders: %s", exc)
        return {"pushes_sent": 0, "error": str(exc)}

    pushes_sent = 0
    skipped = 0

    for bill in bills_resp.data or []:
        days_left = _days_until_due(bill.get("due_date"), now)
        if days_left not in REMINDER_DAYS:
            continue
        uid = str(bill.get("user_id") or "")
        bid = str(bill.get("bill_id") or "")
        if not uid or not bid:
            continue
        ok = send_bill_reminder_push(
            sb,
            user_id=uid,
            bill_id=bid,
            merchant_name=str(bill.get("bill_name") or "Bill"),
            amount_paisa=int(bill.get("amount_paisa") or 0),
            due_date=str(bill.get("due_date") or ""),
            days_until=days_left,
        )
        if ok:
            pushes_sent += 1
        else:
            skipped += 1

    result = {"pushes_sent": pushes_sent, "skipped": skipped, "reminder_days": list(REMINDER_DAYS)}
    logger.info("Bill reminder DAG complete: %s", result)
    return result


def detect_missing_bills(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.api.services.missing_bills import run_missing_bill_detection_for_all_users

    sb = get_supabase_admin_client()
    result = run_missing_bill_detection_for_all_users(sb)
    logger.info("Missing bill detection complete: %s", result)
    return result


with DAG(
    dag_id="bill_reminder_dag",
    description="Phase 14/16: Bill push reminders and missing expected payment detection",
    schedule_interval="0 8,18 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "bills", "push", "phase14", "phase16"],
) as dag:
    send_reminders = PythonOperator(
        task_id="send_bill_reminders",
        python_callable=send_bill_reminders,
    )
    detect_missing = PythonOperator(
        task_id="detect_missing_bills",
        python_callable=detect_missing_bills,
    )
    detect_missing >> send_reminders
