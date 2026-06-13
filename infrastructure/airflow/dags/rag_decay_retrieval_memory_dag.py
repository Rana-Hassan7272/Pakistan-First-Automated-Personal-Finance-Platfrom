"""
P9.8 — Decay stale rows in ``rag_retrieval_memory`` (30-day default).

Manual parity::

    py -3 -c "from backend.api.core.supabase_client import get_supabase_admin_client; \\
from backend.rag.retrieval_memory import decay_stale_retrieval_memory; \\
print(decay_stale_retrieval_memory(get_supabase_admin_client()))"
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

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


def decay_retrieval_memory_task(**_context):
    from backend.api.core.supabase_client import get_supabase_admin_client
    from backend.rag.retrieval_memory import decay_stale_retrieval_memory

    sb = get_supabase_admin_client()
    n = decay_stale_retrieval_memory(sb)
    print(f"decay_retrieval_memory_task removed ~{n} rows (best-effort count)")
    return {"deleted_estimate": n}


with DAG(
    dag_id="rag_decay_retrieval_memory_dag",
    description="Phase 9.5/9.8: delete rag_retrieval_memory rows older than RAG_MEMORY_DECAY_DAYS",
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["rag", "phase9", "maintenance"],
) as dag:
    PythonOperator(
        task_id="decay_stale_retrieval_memory",
        python_callable=decay_retrieval_memory_task,
    )
