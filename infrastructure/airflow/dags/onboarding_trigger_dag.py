from __future__ import annotations

import sys
import os
from datetime import datetime, timezone, timedelta

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


def trigger_onboarding_for_new_users(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.agents.specialist.onboarding_agent import run_onboarding_agent

    sb = get_supabase_admin_client()

    window_start = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    new_users = (
        sb.table("users")
        .select("user_id, email, created_at")
        .gte("created_at", window_start)
        .execute()
        .data or []
    )

    already_onboarded = (
        sb.table("agent_checkpoints")
        .select("user_id")
        .eq("agent_type", "onboarding_advice")
        .execute()
        .data or []
    )
    onboarded_ids = {str(r["user_id"]) for r in already_onboarded}

    pending = [u for u in new_users if str(u["user_id"]) not in onboarded_ids]

    results = {"triggered": 0, "skipped": 0, "failed": 0}
    for user in pending:
        uid = user["user_id"]
        try:
            result = run_onboarding_agent(uid, "Help me get started with my finances")
            sb.table("agent_checkpoints").upsert({
                "thread_id": f"onboarding_{uid}",
                "user_id": uid,
                "checkpoint_data": {
                    "allocation": result.get("allocation"),
                    "zakat_eligibility": result.get("zakat_eligibility"),
                    "response_preview": (result.get("response") or "")[:300],
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
                "agent_type": "onboarding_advice",
            }, on_conflict="thread_id").execute()
            results["triggered"] += 1
        except Exception as exc:
            results["failed"] += 1

    results["skipped"] = len(new_users) - len(pending)
    print(f"Onboarding trigger DAG: new={len(new_users)}, triggered={results['triggered']}, skipped={results['skipped']}, failed={results['failed']}")
    return results


with DAG(
    dag_id="onboarding_trigger_dag",
    description="Poll for new users every 2 hours and trigger onboarding agent",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "agents", "onboarding"],
) as dag:
    PythonOperator(
        task_id="trigger_onboarding_agent_new_users",
        python_callable=trigger_onboarding_for_new_users,
    )
