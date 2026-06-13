# FinGuard API / Celery worker image (optimized runtime)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    FINGUARD_REPO_ROOT=/app

WORKDIR /app

# Minimal runtime libs required by opencv/paddle/torch stacks.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first to maximize docker layer cache reuse.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-compile -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

# Copy only runtime code/config needed by API + worker.
COPY backend/ /app/backend/
COPY configs/ /app/configs/
COPY scripts/ /app/scripts/
COPY evaluation/__init__.py /app/evaluation/__init__.py
COPY evaluation/ingestion/ /app/evaluation/ingestion/

RUN chmod +x /app/scripts/*.sh

ARG HF_ML_REPO_ID=""
RUN if [ -n "$HF_ML_REPO_ID" ]; then \
      HF_ML_REPO_ID="$HF_ML_REPO_ID" \
      FINGUARD_SYNC_ML_ON_START=1 ML_ARTIFACTS_SOURCE=hf \
      python -m backend.deploy.ml_artifacts_hf --force; \
    fi

ENV FINGUARD_SYNC_ML_ON_START=0 \
    FINGUARD_DEFER_PREWARM=1

EXPOSE 8000

CMD ["sh", "scripts/start-api-only.sh"]
