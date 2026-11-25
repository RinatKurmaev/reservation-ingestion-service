"""FastAPI application: POST /reservations and job inspection endpoints."""

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import SessionLocal, init_db
from .idempotency import idempotency_key
from .models import Job, JobState, Reservation
from .mq import Publisher
from .schemas import (
    BulkItemOut,
    JobOut,
    ReservationAccepted,
    ReservationIn,
    ReservationOut,
)

logger = logging.getLogger(__name__)
# The consumer configures logging in main(); under uvicorn nobody does, so the
# app.* loggers (incl. the endpoint-call middleware) would stay silent.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
settings = get_settings()
publisher = Publisher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await publisher.connect()
    yield
    await publisher.close()


app = FastAPI(title="Reservation Ingestion Service", lifespan=lifespan)


# --- DB outages are 503, not 500 ---------------------------------------------
# Every DB-backed endpoint (POST /reservations, GET /jobs*, /reservations, ...)
# goes through this handler when PostgreSQL is unreachable: the *system* is down
# by design, so callers get a retryable 503 instead of an ambiguous 500 — the
# dashboard uses this to show "database unavailable" instead of an error.
#
# Two exception shapes reach it:
# - DBAPIError: server-side/query failures (asyncpg errors are wrapped by
#   SQLAlchemy into DBAPIError subclasses).
# - OSError (covers socket.gaierror, ConnectionRefusedError, timeouts): raw
#   connect-phase failures. SQLAlchemy does NOT wrap these — asyncpg re-raises
#   them as-is when the host doesn't resolve (e.g. `podman compose stop db`
#   removes the container, so the "db" DNS name disappears) or refuses.
async def db_unavailable_handler(request: Request, exc: Exception):
    logger.error("database unavailable for %s %s: %s: %s",
                 request.method, request.url.path, type(exc).__name__, exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable; try again"})


app.add_exception_handler(DBAPIError, db_unavailable_handler)
app.add_exception_handler(OSError, db_unavailable_handler)


# --- Endpoint call logging ----------------------------------------------------
# One INFO line per business-endpoint call: method, path, status, duration.
# /health and /queues are excluded (polled every few seconds by the dashboard
# auto-refresh, they would drown out the interesting calls).
_NO_LOG_PATHS = ("/health", "/queues", "/openapi.json", "/docs", "/redoc")
_SLOW_REQUEST_MS = 1000


@app.middleware("http")
async def log_endpoint_calls(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS" or path.startswith(_NO_LOG_PATHS) or path == "/":
        return await call_next(request)

    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.exception(
            "%s %s -> 500 after %.1f ms (unhandled exception)",
            request.method, path, duration_ms,
        )
        raise
    duration_ms = (time.perf_counter() - start) * 1000
    message = "%s %s -> %d (%.1f ms)"
    args = [request.method, path, response.status_code, duration_ms]
    if duration_ms >= _SLOW_REQUEST_MS:
        logger.warning("SLOW " + message, *args)
    else:
        logger.info(message, *args)
    return response


async def get_session():
    async with SessionLocal() as session:
        yield session


# --- Admission ----------------------------------------------------------------
# Fast path (DB up): commit the job row, then publish (idempotency guarantee =
# UNIQUE constraint on reservations.idempotency_key, evaluated at insert time).
# Outage path ("queue-first", task.md §1): the commit fails because the DB is
# unreachable, so the API answers 202 "queued" and publishes the FULL job spec
# (job_id, idempotency_key, payload, callback_url) — the queue message is the
# durable record. The worker materializes the job row on the first delivery
# where the DB is reachable; until then it nacks into the retry queue (§4.2
# cool-down). Nothing is lost, nothing is double-stored (the UNIQUE constraint
# still arbitrates duplicates that were both admitted during the outage).
class PublishFailed(Exception):
    """The broker did not accept the message (nothing was stored)."""


async def _admit(
    session: AsyncSession,
    payload: dict,
    callback_url: str | None,
) -> tuple[UUID, str]:
    """Admit one validated reservation. Returns (job_id, response_state)."""
    job_id = uuid4()
    idem_key = idempotency_key(payload)
    job = Job(
        id=job_id,
        idempotency_key=idem_key,
        payload=payload,
        state=JobState.pending,
        callback_url=callback_url,
    )
    session.add(job)
    queued = False
    try:
        await session.commit()
    except (DBAPIError, OSError) as exc:
        if isinstance(exc, IntegrityError):
            raise  # a real constraint violation, not an outage
        # Database unavailable: roll back and keep the message as the record.
        await session.rollback()
        queued = True

    message: dict[str, Any] = {"job_id": str(job_id), "payload": payload}
    if queued:
        message.update(
            idempotency_key=idem_key,
            callback_url=callback_url,
            admission="queued",
        )
    try:
        await publisher.publish_job(message)
    except Exception as exc:
        logger.exception("failed to publish job %s", job_id)
        if not queued:
            job.state = JobState.failed
            job.last_error = "publish to queue failed"
            await session.commit()
        raise PublishFailed(f"broker did not accept the job message: {exc}") from exc

    return job_id, "queued" if queued else "pending"


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get(
    "/queues",
    summary="Live queue depths (main/retry/DLQ) via passive declares — RabbitMQ only, no DB",
)
async def queue_depths() -> dict:
    """DB-independent system state for the dashboard: how many messages sit in
    each queue right now. During a DB outage this is the evidence the UI shows
    (e.g. retry queue holding a message cooling down for the 10-min TTL)."""
    stats = await publisher.queue_stats()
    return {
        "queues": stats,
        "webhook_sink": len(_webhook_sink),
    }


# --- Demo webhook sink ------------------------------------------------------
# Lets the scenario dashboard verify webhook delivery end-to-end without an
# external receiver: point callback_url at http://api:8000/hooks/sink (the
# compose service name resolves from the worker container). In-memory, last 50.
_webhook_sink: list[dict] = []


@app.post("/hooks/sink")
async def webhook_sink_receive(payload: dict) -> dict:
    entry = {**payload, "received_at": datetime.now(timezone.utc).isoformat()}
    _webhook_sink.append(entry)
    del _webhook_sink[:-50]
    logger.info("webhook sink received job=%s state=%s", payload.get("job_id"), payload.get("state"))
    return {"received": True}


@app.get("/hooks/sink")
async def webhook_sink_list() -> list[dict]:
    return list(reversed(_webhook_sink))


@app.post(
    "/reservations",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReservationAccepted,
)
async def create_reservation(
    reservation: ReservationIn,
    session: AsyncSession = Depends(get_session),
) -> ReservationAccepted:
    payload = reservation.model_dump(mode="json")
    try:
        job_id, state = await _admit(
            session, payload,
            str(reservation.callback_url) if reservation.callback_url else None,
        )
    except PublishFailed:
        raise HTTPException(status_code=503, detail="could not enqueue reservation; try again")
    return ReservationAccepted(job_id=job_id, state=state)


BULK_MAX_ITEMS = 100


@app.post(
    "/reservations/bulk",
    status_code=207,  # Multi-Status: admission is per-item, the batch can be partial
    response_model=list[BulkItemOut],
    summary="Bulk-submit reservations; each item is admitted (or rejected) independently",
)
async def create_reservations_bulk(
    reservations: list[dict[str, Any]] = Body(..., examples=[
        [{
            "checkin": "2026-07-01", "checkout": "2026-07-05", "num_guests": 2,
            "guest_names": ["Ada", "Linus"],
        }]
    ]),
    session: AsyncSession = Depends(get_session),
) -> list[BulkItemOut]:
    """Admit a batch of reservations, one job per item.

    An invalid item does NOT reject the batch: it is reported as `accepted: false`
    with the validation error, and the caller can fix + re-submit just that item.
    Every item gets its own job/message, so a mid-batch failure never blocks or
    loses the other items; re-submitting the whole batch is safe (idempotency).
    A DB outage does NOT abort the batch either: each item falls back to
    queue-first admission (202-equivalent, accepted: true) — see `_admit`.
    """
    if not reservations:
        raise HTTPException(status_code=422, detail="empty batch")
    if len(reservations) > BULK_MAX_ITEMS:
        raise HTTPException(status_code=422, detail=f"batch too large (max {BULK_MAX_ITEMS} items)")

    results: list[BulkItemOut] = []
    for index, raw in enumerate(reservations):
        try:
            reservation = ReservationIn.model_validate(raw)
        except ValidationError as exc:
            detail = "; ".join(
                f"{'.'.join(str(loc) for loc in err['loc']) or 'payload'}: {err['msg']}"
                for err in exc.errors()
            )
            results.append(BulkItemOut(index=index, accepted=False, detail=detail))
            continue

        payload = reservation.model_dump(mode="json")
        try:
            job_id, _state = await _admit(
                session, payload,
                str(reservation.callback_url) if reservation.callback_url else None,
            )
        except PublishFailed as exc:
            logger.exception("failed to publish bulk job (item %d)", index)
            results.append(
                BulkItemOut(index=index, accepted=False, detail="publish to queue failed; try again")
            )
            continue
        except (DBAPIError, OSError) as exc:
            # An admission error that is NOT an outage (e.g. a constraint
            # violation) — report the item as rejected instead of aborting.
            logger.exception("bulk admission failed for item %d", index)
            await session.rollback()
            results.append(
                BulkItemOut(index=index, accepted=False, detail=f"admission failed: {exc}")
            )
            continue

        results.append(BulkItemOut(index=index, accepted=True, job_id=job_id))

    return results


@app.get("/reservations", response_model=list[ReservationOut])
async def list_reservations(
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[Reservation]:
    result = await session.execute(
        select(Reservation).order_by(Reservation.created_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


@app.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/jobs", response_model=list[JobOut])
async def list_jobs(
    state: JobState | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[Job]:
    query = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if state is not None:
        query = query.where(Job.state == state)
    result = await session.execute(query)
    return list(result.scalars().all())


@app.post("/jobs/{job_id}/requeue", response_model=JobOut)
async def requeue_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.state not in {JobState.pending, JobState.dead_lettered, JobState.failed}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"only pending/dead_lettered/failed jobs can be requeued (job is {job.state.value})",
        )

    job.state = JobState.pending
    job.retry_count = 0
    job.last_error = None
    await session.commit()

    try:
        await publisher.publish_job(
            {"job_id": str(job.id), "payload": job.payload},
            headers={"x-retry-count": 0},
        )
    except Exception:
        logger.exception("failed to re-publish job %s", job.id)
        job.state = JobState.failed
        job.last_error = "publish to queue failed during requeue"
        await session.commit()
        raise HTTPException(status_code=503, detail="could not re-enqueue job; try again")

    logger.info("job %s requeued for processing", job.id)
    return job


# --- Demo reset (scenario dashboard) ------------------------------------------
# Wipes all demo state so the scenarios can start "fresh": purges every queue
# (main / retry / DLQ), truncates the jobs + reservations tables and clears the
# in-memory webhook sink. Queues go first so no in-flight message can re-create
# rows after the truncate; a message that is being processed RIGHT NOW may still
# land afterwards — for the demo stack that race is acceptable.
@app.post("/admin/reset", summary="Demo reset: purge queues, wipe DB rows, clear the webhook sink")
async def reset_system() -> dict:
    errors: list[str] = []

    # 1. RabbitMQ: drop all queued messages (main, retry cool-down, DLQ).
    purged: dict[str, int] = {}
    try:
        purged = await publisher.purge_queues()
    except Exception as exc:
        errors.append(f"queue purge failed: {exc}")
        logger.exception("queue purge failed during /admin/reset")

    # 2. Database: report what is being removed, then truncate both tables.
    deleted: dict[str, int] = {"jobs": 0, "reservations": 0}
    try:
        async with SessionLocal() as session:
            for table in deleted:
                deleted[table] = (await session.execute(
                    text(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed names
                )).scalar_one()
            await session.execute(text("TRUNCATE reservations, jobs"))
            await session.commit()
    except Exception as exc:
        errors.append(f"db truncate failed: {exc}")
        logger.exception("db truncate failed during /admin/reset")

    # 3. In-memory webhook sink (this process).
    sink_cleared = len(_webhook_sink)
    _webhook_sink.clear()

    logger.info("system reset: queues=%s deleted=%s sink=%d errors=%s",
                purged, deleted, sink_cleared, errors or "none")
    return {
        "ok": not errors,
        "queues_purged": purged,
        "rows_deleted": deleted,
        "webhook_sink_cleared": sink_cleared,
        "errors": errors,
    }
