"""
P9.8 — Manual / scheduled full KB ingest (PDFs → Supabase + BM25).

**Operator:** ensure worker has ``backend/rag/pdfdata``, Supabase credentials, and enough CPU/RAM.
Default is a no-op shell that prints the command; set ``RAG_INGEST_DRY_RUN=0`` to execute.

Manual parity (full rebuild is the CLI default; use ``--incremental`` for incremental)::

    py -3 -m backend.rag.scripts.ingest_kb
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

_DEFAULT_ARGS = {
    "owner": "finguard",
    "retries": 0,
    "email_on_failure": False,
}

_dry = os.getenv("RAG_INGEST_DRY_RUN", "1").strip().lower() in ("1", "true", "yes")
_cmd = (
    f'echo "RAG_INGEST_DRY_RUN=1 (set to 0 to run): cd {_repo_root} && '
    f'{sys.executable} -m backend.rag.scripts.ingest_kb"'
    if _dry
    else f"cd {_repo_root} && {sys.executable} -m backend.rag.scripts.ingest_kb"
)

with DAG(
    dag_id="rag_kb_ingest_manual_dag",
    description="Phase 9.8: full RAG KB ingest (dry-run by default via RAG_INGEST_DRY_RUN)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["rag", "phase9", "ingest"],
) as dag:
    BashOperator(
        task_id="ingest_kb_placeholder_or_run",
        bash_command=_cmd,
    )
