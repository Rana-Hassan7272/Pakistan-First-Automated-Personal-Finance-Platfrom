"""
P9.6 / P9.8 — Nightly user spending summaries → ``rag_user_memory_chunks`` (384-d embeddings).

Manual parity::

    py -3 -c "from backend.rag.user_memory_etl import run_nightly_user_memory_sync; \\
print(run_nightly_user_memory_sync())"
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


def sync_user_memory_chunks_task(**_context):
    from backend.rag.user_memory_etl import run_nightly_user_memory_sync

    return run_nightly_user_memory_sync()


with DAG(
    dag_id="rag_user_memory_nightly_dag",
    description="Phase 9.6: aggregate debits → template summary → embed → rag_user_memory_chunks",
    schedule_interval="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["rag", "phase9", "user_memory"],
) as dag:
    PythonOperator(
        task_id="sync_user_memory_chunks",
        python_callable=sync_user_memory_chunks_task,
    )
