# AGENTS.md

## What this is

Reservation Ingestion Service: FastAPI API + async queue worker, PostgreSQL (SQLAlchemy 2 async + asyncpg), RabbitMQ (aio-pika). `task.md` is the spec; `README.md` documents topology, failure strategy, and API examples — keep both in sync with behavior.

## Layout

- `app/api.py` — FastAPI app (`/reservations`, `/jobs`, `/jobs/{id}/requeue`). Runs via uvicorn.
- `app/consumer.py` — queue worker (`python -m app.consumer`). Separate process; runs the reaper task.
- `app/mq.py` — RabbitMQ topology (main/retry/DLQ) + publisher. `app/db.py` — engine, `create_all` (no Alembic).
- `app/errors.py` — failure classifier the worker relies on.
- No test suite. Quick check: `python3 -m compileall -q app && rm -rf app/__pycache__`. Real verification = running the compose stack and curling the API.

## Podman (not Docker) quirks

- Stack: `podman compose up -d --build`. After code changes you MUST run `podman compose build` first — podman-compose 1.3 does not rebuild an image that already exists, so `up --build` alone silently runs stale code.
- Images in `compose.yaml` are fully qualified (`docker.io/library/...`): podman has no default search registry.
- RabbitMQ needs its named volume (`rabbitmq_data:/var/lib/rabbitmq`); under rootless podman it crashes with `.erlang.cookie: eacces` otherwise.
- From inside containers, reach host services via `host.containers.internal` (e.g. webhook `callback_url`).
- podman-compose can be slow; long `down`/`up` commands time out in foreground shells — run them in the background.

## aio-pika 9.x trap (pinned `<10` on purpose)

`channel.confirm_delivery()` was removed in aio-pika 9/10. Publisher confirms are enabled by default and `exchange.publish()` *returns* the broker frame — check it is `Ack` (see `Publisher._check_confirmed`). Do not "restore" confirm_delivery.

## Async SQLAlchemy traps

- Never read a column with server-side `onupdate`/`server_default` (e.g. `Job.updated_at`) after `commit()` — it triggers a lazy refresh and raises `MissingGreenlet`. Set `updated_at = _now()` client-side at each state transition (the worker does this everywhere).
- After a `rollback()`, reload the row via `reload_job()` (select with `populate_existing=True`) — expired attribute access from async context raises.

## Consumer correctness rules

- The UNIQUE constraint on `reservations.idempotency_key` is the dedupe guarantee. Insert + job-state update in one transaction; ACK only after commit. `IntegrityError` → `duplicate` (or idempotent re-completion when `existing.job_id == job.id`).
- Failure classification lives in `app/errors.py`: unknown exceptions default to transient; SQLSTATE `22xxx`/`23xxx` (except `23505` duplicate) are permanent. `23505` is handled as duplicate/completed, not an error.
- Transient retries publish a copy to the retry queue with per-message TTL (`expiration`), then ACK — never sleep/retry in-process. During a full DB outage the handler can't read the job row, so messages nack into the retry queue and get the queue-level TTL (10 min) as cool-down; this is expected, not a stuck message.
- Queue declarations in `mq.py` must stay identical everywhere they are re-declared (wrong/changed queue args → RabbitMQ PRECONDITION_FAILED, channel dies). Changing queue TTL requires deleting the queue first.

## Env / infra

- Settings via env (`DATABASE_URL`, `RABBITMQ_URL`) with sane compose defaults; see `app/config.py`.
- Health checks: `GET /health`, and `podman ps` (db/rabbitmq are health-gated via `depends_on`).
- Queue depth debug: `podman exec reservation-ingestion_rabbitmq_1 rabbitmqctl list_queues name messages messages_unacknowledged`.
