"""Queue worker: consumes reservation jobs, stores them idempotently, fires webhooks.

Run with: python -m app.consumer

Failure strategy:
- transient failure -> publish a retry copy to the retry queue (per-message TTL,
  exponential backoff + jitter) and ACK; the worker never sleeps on retries.
- retries exhausted -> mark job `dead_lettered`, park the message in the DLQ, ACK.
- permanent failure -> mark job `failed`, ACK (no retries).
- unique-violation on reservations.idempotency_key -> `duplicate` (or an
  idempotent replay-completion if the same job was redelivered after a crash).
- unparseable/unknown messages -> parked in the DLQ with error headers.
- a reaper requeues job rows stuck in `processing` beyond the lease timeout.
"""

import asyncio
import json
import logging
import random
import signal
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import aio_pika
import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .db import SessionLocal, init_db
from .errors import PermanentError, classify_exception, is_unique_violation
from .models import TERMINAL_STATES, Job, JobState, Reservation
from .mq import Publisher, Topology, declare_topology
from .webhook import schedule_webhook

logger = logging.getLogger("app.consumer")
settings = get_settings()


def reservation_kwargs(payload: dict) -> dict:
    """Convert the JSON payload into Reservation columns.

    Raises KeyError/TypeError/ValueError on malformed payloads; the caller
    classifies those as permanent.
    """
    return {
        "checkin": date.fromisoformat(payload["checkin"]),
        "checkout": date.fromisoformat(payload["checkout"]),
        "num_guests": int(payload["num_guests"]),
        "guest_names": [str(name) for name in payload["guest_names"]],
    }


def _now() -> datetime:
    """Timestamps are set client-side at each transition: reading the server-side
    onupdate value after commit would trigger a lazy refresh (MissingGreenlet)."""
    return datetime.now(timezone.utc)


def webhook_payload(job: Job) -> dict:
    body: dict = {
        "job_id": str(job.id),
        "state": job.state.value,
        "finished_at": job.updated_at.isoformat() if job.updated_at else None,
    }
    if job.reservation_id is not None:
        body["reservation_id"] = str(job.reservation_id)
    if job.last_error:
        body["error"] = job.last_error
    return body


async def reload_job(session: AsyncSession, job_id: UUID) -> Job:
    """Re-load a job after a rollback (rolls back expire instances)."""
    result = await session.execute(
        select(Job).where(Job.id == job_id).execution_options(populate_existing=True)
    )
    return result.scalar_one()


class Worker:
    def __init__(self) -> None:
        self.publisher = Publisher()
        self.http = httpx.AsyncClient()
        self._connection: aio_pika.RobustConnection | None = None
        self._reaper_task: asyncio.Task | None = None

    async def start(self) -> None:
        await self.publisher.connect()
        self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=settings.prefetch_count)
        topology: Topology = await declare_topology(channel)
        self._reaper_task = asyncio.create_task(reaper_loop(self), name="stuck-job-reaper")
        logger.info("worker consuming from %s (prefetch=%d)", settings.main_queue, settings.prefetch_count)
        await topology.main_queue.consume(self.on_message)

    async def stop(self) -> None:
        if self._reaper_task is not None:
            self._reaper_task.cancel()
        if self._connection is not None:
            await self._connection.close()
        await self.publisher.close()
        await self.http.aclose()

    def fire_webhook(self, job: Job) -> None:
        if not job.callback_url:
            return
        schedule_webhook(self.http, job.callback_url, webhook_payload(job))

    async def on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            await self._handle(message)
        except Exception:
            # Last-resort net: the nack dead-letters into the retry queue, so the
            # message comes back after a delay instead of being lost.
            logger.exception("unexpected error handling message; nacking for delayed redelivery")
            await message.nack(requeue=False)

    async def _handle(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        body = message.body
        try:
            parsed = json.loads(body)
            job_id = UUID(str(parsed["job_id"]))
            payload = dict(parsed["payload"])
        except Exception as exc:
            await self._dead_letter(message, body, f"unparseable message: {exc}")
            return

        async with SessionLocal() as session:
            job = (
                await session.execute(select(Job).where(Job.id == job_id))
            ).scalar_one_or_none()
            if job is None:
                logger.error("message references unknown job %s; dead-lettering", job_id)
                await self._dead_letter(message, body, f"unknown job id {job_id}")
                return
            if job.state in TERMINAL_STATES:
                logger.info(
                    "job %s already in terminal state %s; acking redundant delivery",
                    job_id, job.state.value,
                )
                await message.ack()
                return

            # Reconcile retry count from the message header (survives restarts).
            header_retry = int((message.headers or {}).get("x-retry-count") or 0)
            if header_retry > job.retry_count:
                job.retry_count = header_retry
            job.state = JobState.processing
            job.updated_at = _now()
            await session.commit()

            try:
                kwargs = reservation_kwargs(payload)
            except (KeyError, TypeError, ValueError) as exc:
                job = await reload_job(session, job_id)
                job.state = JobState.failed
                job.updated_at = _now()
                job.last_error = f"invalid payload: {exc}"
                await session.commit()
                logger.error("job %s failed permanently: invalid payload (%s)", job_id, exc)
                self.fire_webhook(job)
                await message.ack()
                return

            try:
                # Insert + state transition in ONE transaction; the UNIQUE
                # constraint on idempotency_key is the dedupe guarantee.
                reservation = Reservation(
                    job_id=job.id, idempotency_key=job.idempotency_key, **kwargs
                )
                session.add(reservation)
                await session.flush()
                job.state = JobState.completed
                job.updated_at = _now()
                job.reservation_id = reservation.id
                job.last_error = None
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                await self._on_integrity_error(session, job_id, message, exc)
                return
            except Exception as exc:
                await session.rollback()
                await self._on_failure(session, job_id, message, exc)
                return

            logger.info("job %s completed (reservation %s)", job.id, job.reservation_id)
            self.fire_webhook(job)
            await message.ack()

    async def _on_integrity_error(
        self, session: AsyncSession, job_id: UUID, message, exc: IntegrityError
    ) -> None:
        if is_unique_violation(exc):
            job = await reload_job(session, job_id)
            existing = (
                await session.execute(
                    select(Reservation).where(Reservation.idempotency_key == job.idempotency_key)
                )
            ).scalar_one_or_none()
            if existing is not None and existing.job_id == job.id:
                # Same job redelivered after a crash between commit and ack:
                # the insert already happened, so this replay just completes it.
                job.state = JobState.completed
                job.updated_at = _now()
                job.reservation_id = existing.id
                job.last_error = None
                logger.info("job %s completed idempotently after redelivery", job.id)
            else:
                job.state = JobState.duplicate
                job.updated_at = _now()
                if existing is not None:
                    job.reservation_id = existing.id
                logger.info(
                    "job %s marked duplicate of reservation %s",
                    job.id, getattr(existing, "id", None),
                )
            await session.commit()
            self.fire_webhook(job)
            await message.ack()
            return

        await self._fail_permanently(session, job_id, message, exc)

    async def _on_failure(
        self, session: AsyncSession, job_id: UUID, message, exc: Exception
    ) -> None:
        if classify_exception(exc) is PermanentError:
            await self._fail_permanently(session, job_id, message, exc)
            return
        await self._retry(session, job_id, message, exc)

    async def _fail_permanently(
        self, session: AsyncSession, job_id: UUID, message, exc: BaseException
    ) -> None:
        job = await reload_job(session, job_id)
        job.state = JobState.failed
        job.updated_at = _now()
        job.last_error = f"{type(exc).__name__}: {exc}"
        await session.commit()
        logger.error("job %s failed permanently: %s", job_id, exc)
        self.fire_webhook(job)
        await message.ack()

    async def _retry(
        self, session: AsyncSession, job_id: UUID, message, exc: BaseException
    ) -> None:
        job = await reload_job(session, job_id)
        job.retry_count += 1
        job.updated_at = _now()
        job.last_error = f"{type(exc).__name__}: {exc}"[:2000]
        logger.warning(
            "job %s transient failure (retry %d/%d): %s",
            job_id, job.retry_count, settings.max_retries, exc,
        )

        if job.retry_count > settings.max_retries:
            job.state = JobState.dead_lettered
            job.updated_at = _now()
            await session.commit()
            logger.error("job %s dead-lettered after %d failed retries", job_id, settings.max_retries)
            await self.publisher.publish_to_dlq(
                message.body,
                headers={
                    "x-retry-count": job.retry_count,
                    "x-error": job.last_error,
                    "x-reason": "max-retries-exceeded",
                },
            )
            self.fire_webhook(job)
            await message.ack()
            return

        # Persist the count BEFORE redelivering so it survives crashes.
        await session.commit()
        delay = min(
            settings.backoff_base_seconds * 2 ** (job.retry_count - 1),
            settings.backoff_cap_seconds,
        )
        delay += random.uniform(0, settings.backoff_base_seconds)
        await self.publisher.publish_retry(
            message.body,
            headers={"x-retry-count": job.retry_count, "x-last-error": job.last_error},
            delay_seconds=delay,
        )
        await message.ack()
        logger.info("job %s scheduled for retry #%d in %.1fs", job_id, job.retry_count, delay)

    async def _dead_letter(self, message, body: bytes, reason: str) -> None:
        logger.error("dead-lettering message (%s): %s", reason, body[:500])
        try:
            await self.publisher.publish_to_dlq(
                body, headers={"x-error": reason, "x-reason": "unprocessable"}
            )
        except Exception:
            logger.exception("could not publish to DLQ; nacking for redelivery")
            await message.nack(requeue=True)
            return
        await message.ack()


async def reaper_loop(worker: Worker) -> None:
    """Requeue job rows stuck in `processing` beyond the lease timeout
    (their messages were lost with the consumer that died mid-processing)."""
    while True:
        await asyncio.sleep(settings.reaper_interval_seconds)
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.lease_timeout_seconds)
            async with SessionLocal() as session:
                result = await session.execute(
                    select(Job).where(
                        Job.state == JobState.processing,
                        Job.updated_at < cutoff,
                    )
                )
                stale = result.scalars().all()
                if not stale:
                    continue
                for job in stale:
                    job.state = JobState.pending
                    job.updated_at = _now()
                await session.commit()
                for job in stale:
                    await worker.publisher.publish_job(
                        {"job_id": str(job.id), "payload": job.payload},
                        headers={"x-retry-count": job.retry_count},
                    )
                    logger.warning("reaper requeued job %s stuck in processing", job.id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("reaper iteration failed")


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    await init_db()
    worker = Worker()
    await worker.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stop.wait()
    finally:
        await worker.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
