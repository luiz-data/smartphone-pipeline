# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Data pipeline that collects smartphone listings from the Mercado Livre API and processes them through a multi-stage architecture: **Collector → Redis Stream → Consumer → PostgreSQL → dbt → Streamlit Dashboard**, orchestrated by Prefect.

## Setup

```bash
cp .env.example .env   # fill in ML_APP_ID, ML_CLIENT_SECRET, POSTGRES_PASSWORD, etc.
docker-compose up      # starts all 6 services
```

Required env vars: `ML_APP_ID`, `ML_CLIENT_SECRET`, `POSTGRES_PASSWORD` — the rest have sensible defaults in `.env.example`.

## Running services individually

```bash
docker-compose up postgres redis          # infrastructure only
docker-compose up collector               # run one collection batch
docker-compose logs -f collector          # follow JSON-formatted logs
```

To run the collector locally (outside Docker):

```bash
cd collector
pip install -r requirements.txt
# ensure .env exists at project root or export vars manually
python main.py
```

## Architecture

### Data flow

```
Mercado Livre API
    ↓ (httpx + tenacity retry)
collector/              → publishes to Redis Stream (smartphones_raw)
    ↓                      failed messages → Redis Stream (smartphones_failed)
consumer/               → reads Redis, writes to PostgreSQL raw schema  [NOT YET IMPLEMENTED]
    ↓
dbt/                    → raw → staging → marts transformations          [NOT YET IMPLEMENTED]
    ↓
dashboard/              → Streamlit on port 8501                         [NOT YET IMPLEMENTED]
```

Prefect (port 4200) orchestrates scheduling; it stores its own state in the same PostgreSQL database.

### PostgreSQL schemas

Three-layer medallion architecture created by `infra/init.sql`:
- `raw` — landing zone for Consumer writes
- `staging` — cleaned/typed dbt models
- `marts` — aggregated analytics tables

### Collector internals (`collector/`)

| File | Responsibility |
|------|---------------|
| `config.py` | Loads all env vars at import time; fails fast if any are missing |
| `mercadolivre.py` | `fetch_page()` with tenacity retry (3 attempts, 30–120s backoff); `collect_all()` generator iterates pages |
| `publisher.py` | Singleton Redis connection; publishes to `REDIS_STREAM`, falls back to `REDIS_FAILED_STREAM` on error |
| `logger.py` | JSON-structured logger — all log calls use `extra={"extra": {...}}` |
| `main.py` | Entry point: generates `batch_id`, iterates `collect_all()`, calls `publish()` per product |

All product fields are extracted in `_extract_fields()` in `mercadolivre.py` — this is the single source of truth for the schema.

### Implemented vs planned

- **Implemented:** `collector/` (fully functional)
- **Placeholder (no code yet):** `consumer/`, `dashboard/`, `orchestration/`, `dbt/models/`, `tests/`

## Key conventions

- All configuration comes from environment variables via `config.py`; never hardcode values.
- Logs use structured JSON with `extra={"extra": {}}` nesting (see `logger.py` for the formatter).
- Redis connection in `publisher.py` is a module-level singleton — do not create per-call connections.
- The consumer should follow the same pattern: read from `REDIS_STREAM`, acknowledge messages, write to `raw` schema, send failures to `REDIS_FAILED_STREAM`.
