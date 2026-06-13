"""
infrastructure/airflow/dags/zakat_eligibility_dag.py

Phase 11: Zakat eligibility notification DAG (Hijri-aware).

Schedule: Monthly on 1st at midnight (0 0 1 * *)
Logic:
- Use hijri-converter to determine current Hijri month
- Check each user's hawl (zakat year) start date
- If hawl completes this month → insert Zakat notification

Hawl = 1 lunar year (354 days) from when nisab was first held
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

try:
    from hijri_converter import convert
    HIJRI_AVAILABLE = True
except ImportError:
    HIJRI_AVAILABLE = False
    logging.warning("hijri-converter not installed, Zakat DAG will use Gregorian approximation")

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

# Phase 11: Hawl is approximately 354 days (lunar year)
HAWL_DAYS = 354


def _get_hijri_today() -> tuple[int, int]:
    """Get current Hijri month and year."""
    if HIJRI_AVAILABLE:
        hijri = convert.Gregorian.today().to_hijri()
        return hijri.year, hijri.month
    else:
        # Fallback: approximate (less accurate)
        today = datetime.now(timezone.utc)
        # Approximate Hijri is about 11 days shorter per year
        approx_hijri_month = ((today.year - 622) * 12 + today.month) % 12 + 1
        return today.year, approx_hijri_month


def _is_hawl_complete(hawl_start: str) -> bool:
    """Check if hawl (354 days) has passed since start date."""
    try:
        start = datetime.fromisoformat(hawl_start.replace("Z", "+00:00"))
        hawl_end = start + timedelta(days=HAWL_DAYS)
        return datetime.now(timezone.utc) >= hawl_end
    except Exception:
        return False


def _already_notified_this_month(sb, user_id: str) -> bool:
    """Check if user was already notified about Zakat this month."""
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0)

    try:
        resp = (
            sb.table("notifications")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .eq("type", "zakat_reminder")
            .gte("created_at", month_start.isoformat())
            .execute()
        )
        return (resp.count or 0) > 0
    except Exception:
        return False


def check_zakat_eligibility(**context):
    """
    Phase 11: Check Zakat hawl completion and send notifications.
    """
    from backend.api.core.supabase_client import get_supabase_admin_client

    sb = get_supabase_admin_client()

    # Get current Hijri date
    hijri_year, hijri_month = _get_hijri_today()
    logger.info(f"Current Hijri date: {hijri_year}-{hijri_month}")

    # Get users with zakat settings configured
    users_resp = (
        sb.table("user_zakat_settings")
        .select("user_id, hawl_start_date, nisab_threshold_paisa, is_eligible")
        .eq("is_eligible", True)
        .execute()
    )
    users = users_resp.data or []

    notified = 0
    skipped = 0
    ineligible = 0
    failed = 0

    for user in users:
        uid = user["user_id"]
        hawl_start = user.get("hawl_start_date")

        if not hawl_start:
            ineligible += 1
            continue

        # Check if hawl is complete
        if not _is_hawl_complete(hawl_start):
            logger.debug(f"Hawl not complete for {uid}")
            ineligible += 1
            continue

        # Skip if already notified this month
        if _already_notified_this_month(sb, uid):
            logger.debug(f"Already notified {uid} this month")
            skipped += 1
            continue

        try:
            # Calculate Zakat due (2.5% of nisab or actual wealth, whichever is higher)
            nisab = user.get("nisab_threshold_paisa", 0) / 100  # Convert to PKR

            # Get user's current balance/assets from transactions
            balance_resp = (
                sb.table("transactions")
                .select("amount_paisa, transaction_type")
                .eq("user_id", uid)
                .gte("transaction_date", hawl_start)
                .execute()
            )
            txns = balance_resp.data or []

            # Rough wealth estimation (sum of credits - debits)
            wealth = sum(
                t["amount_paisa"] if t.get("transaction_type") == "credit" else -t["amount_paisa"]
                for t in txns
            ) / 100

            zakat_base = max(wealth, nisab)
            zakat_due = zakat_base * 0.025  # 2.5%

            # Insert notification
            sb.table("notifications").insert({
                "user_id": uid,
                "type": "zakat_reminder",
                "title": "Zakat Due - Hawl Complete",
                "message": f"Your hawl is complete. Estimated Zakat: Rs. {zakat_due:.2f} on wealth of Rs. {zakat_base:.2f}",
                "reference_id": f"zakat_{hijri_year}_{hijri_month}",
                "reference_type": "zakat_eligibility",
                "action_url": "/zakat",
                "payload_json": {
                    "hawl_start": hawl_start,
                    "hijri_year": hijri_year,
                    "hijri_month": hijri_month,
                    "estimated_zakat_pkr": zakat_due,
                    "wealth_basis_pkr": zakat_base,
                },
                "is_read": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }).execute()

            # Update user's last zakat reminder
            sb.table("user_zakat_settings").update({
                "last_reminder_sent_at": datetime.now(timezone.utc).isoformat(),
                "hawl_start_date": datetime.now(timezone.utc).isoformat(),  # Reset for next year
            }).eq("user_id", uid).execute()

            logger.info(f"Sent Zakat notification to {uid}")
            notified += 1

        except Exception as exc:
            logger.error(f"Failed to send Zakat notification to {uid}: {exc}")
            failed += 1

    result = {
        "notified": notified,
        "skipped_duplicate": skipped,
        "ineligible": ineligible,
        "failed": failed,
        "hijri_year": hijri_year,
        "hijri_month": hijri_month,
    }
    logger.info(f"Zakat eligibility DAG complete: {result}")
    return result


with DAG(
    dag_id="zakat_eligibility_dag",
    description="Monthly Zakat eligibility check (Hijri-aware) - 1st of month at midnight (Phase 11)",
    schedule_interval="0 0 1 * *",  # Monthly on 1st at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "zakat", "islamic", "notifications", "phase11"],
) as dag:
    PythonOperator(
        task_id="check_zakat_eligibility",
        python_callable=check_zakat_eligibility,
        doc_md="Check hawl completion per user and insert Zakat notifications.",
    )
