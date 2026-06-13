from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from dotenv import load_dotenv
load_dotenv(os.path.join(_repo_root, ".env"))

_DEFAULT_ARGS = {
    "owner": "finguard",
    "retries": 1,
    "email_on_failure": False,
}


def _has_recent_transactions(sb, user_id: str, days: int = 30) -> bool:
    """Phase 11: Check if user has transactions in the last N days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    resp = (
        sb.table("transactions")
        .select("transaction_id", count="exact")
        .eq("user_id", user_id)
        .gte("transaction_date", cutoff)
        .limit(1)
        .execute()
    )
    return (resp.count or 0) > 0


def run_health_for_all_users(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.agents.specialist.health_agent import run_health_agent

    sb = get_supabase_admin_client()
    users = sb.table("users").select("user_id").execute().data or []

    # Phase 11: Mark existing scores as stale before computing new ones
    stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    try:
        sb.table("health_score_history").update({"is_stale": True}).lt(
            "computed_at", stale_cutoff
        ).execute()
    except Exception as exc:
        print(f"Warning: Could not mark stale scores: {exc}")

    results = {"success": 0, "failed": 0, "skipped_no_tx": 0}
    for user in users:
        uid = user["user_id"]
        try:
            # Phase 11: Skip users with no recent transactions (zero-tx guard)
            if not _has_recent_transactions(sb, uid, days=30):
                print(f"Skipping user {uid}: no transactions in last 30 days")
                results["skipped_no_tx"] += 1
                continue

            result = run_health_agent(uid, "Calculate my financial health score")
            score = result.get("card_data", {}).get("total_score")
            if score is not None:
                sb.table("health_score_history").upsert({
                    "user_id": uid,
                    "total_score": score,
                    "grade": result["card_data"].get("grade", "?"),
                    "components": result["card_data"].get("components", {}),
                    "computed_at": datetime.now(timezone.utc).isoformat(),
                    "is_stale": False,  # Phase 11: Fresh score
                }, on_conflict="user_id,computed_at").execute()
            results["success"] += 1
        except Exception as exc:
            print(f"Health agent failed for {uid}: {exc}")
            results["failed"] += 1

    print(f"Health DAG complete: {results}")
    return results


with DAG(
    dag_id="financial_health_sunday_dag",
    description="Run Health Agent for all users every Sunday midnight (Phase 11: zero-tx skip + stale flag)",
    schedule_interval="0 0 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "agents", "health"],
) as dag:
    PythonOperator(
        task_id="run_health_agent_all_users",
        python_callable=run_health_for_all_users,
    )
