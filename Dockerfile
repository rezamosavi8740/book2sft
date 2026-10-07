FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend
COPY tools /app/tools

RUN mkdir -p /data/jobs

EXPOSE 8088

CMD ["sh", "-c", "uvicorn backend.main:app --host ${BOOK2SFT_HOST:-0.0.0.0} --port ${BOOK2SFT_PORT:-8088} --workers 1"]
