"""
Sunday morning spending insights + Expo push (Phase 14).

Schedule: Sundays at 8:00 AM — top weekly insight push per user.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

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


def _insight_headline(finding: dict) -> str:
    name = str(finding.get("name") or "insight").replace("_", " ")
    value = finding.get("value")
    unit = finding.get("unit") or ""
    if unit == "%":
        return f"Your {name} is {value}% this period."
    if unit == "bool" and value:
        return f"Heads up: {name.replace('_', ' ')} detected."
    if unit == "hour":
        return f"Peak spending hour: {value}:00."
    if unit == "direction":
        return f"Spending trend: {value}."
    return f"{name}: {value} {unit}".strip()


def refresh_spending_dna_benchmarks(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.api.services.spending_dna import rebuild_population_benchmarks

    sb = get_supabase_admin_client()
    result = rebuild_population_benchmarks(sb)
    print(f"Spending DNA benchmarks: {result}")
    return result


def run_insights_for_all_users(**context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.agents.specialist.insights_agent import run_insights_agent
    from backend.api.services.expo_push import send_weekly_insight_push

    sb = get_supabase_admin_client()
    users = sb.table("users").select("user_id").execute().data or []

    results = {"success": 0, "failed": 0, "pushes": 0}
    for user in users:
        uid = user["user_id"]
        try:
            result = run_insights_agent(uid, "Give me my spending insights")
            top_insights = result.get("top_insights", [])
            if top_insights:
                sb.table("agent_checkpoints").upsert(
                    {
                        "thread_id": f"insights_nightly_{uid}",
                        "user_id": uid,
                        "checkpoint_data": {
                            "top_insights": top_insights,
                            "computed_at": datetime.now(timezone.utc).isoformat(),
                        },
                        "agent_type": "spending_insights",
                    },
                    on_conflict="thread_id",
                ).execute()
                headline = _insight_headline(top_insights[0])
                if send_weekly_insight_push(sb, user_id=uid, headline=headline):
                    results["pushes"] += 1
            results["success"] += 1
        except Exception as exc:
            print(f"Insights failed for {uid}: {exc}")
            results["failed"] += 1

    print(f"Insights Sunday DAG complete: {results}")
    return results


with DAG(
    dag_id="spending_insights_nightly_dag",
    description="Phase 14: Sunday 8am insights agent + top insight Expo push",
    schedule_interval="0 8 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["finguard", "agents", "insights", "push", "phase14"],
) as dag:
    PythonOperator(
        task_id="refresh_spending_dna_benchmarks",
        python_callable=refresh_spending_dna_benchmarks,
    )
    PythonOperator(
        task_id="run_insights_agent_all_users",
        python_callable=run_insights_for_all_users,
    )
