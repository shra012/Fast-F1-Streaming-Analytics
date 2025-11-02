# syntax=docker/dockerfile:1.7

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    FASTF1_CACHE_DIR=/app/.fastf1-cache

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt pyproject.toml uv.lock ./
COPY producer ./producer

RUN pip install --no-cache-dir -r requirements.txt \
 && mkdir -p "${FASTF1_CACHE_DIR}"

ENTRYPOINT ["python", "producer/simple_producer.py"]
