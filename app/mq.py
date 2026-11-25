"""RabbitMQ topology (main / retry / DLQ) and a publisher with confirms.

Flow of a message:

    main exchange ──> reservations.jobs ──(transient failure: publish w/ TTL)──> retry exchange
                          ▲                                                          │
                          │ (TTL expired, dead-lettered back)                        ▼
                          └────────────────────────────────────────────── reservations.retry

    exhausted retries / poison messages ──> reservations.dlx ──> reservations.dlq
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from .config import get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Topology:
    main_exchange: aio_pika.RobustExchange
    retry_exchange: aio_pika.RobustExchange
    dlx: aio_pika.RobustExchange
    main_queue: aio_pika.RobustQueue
    retry_queue: aio_pika.RobustQueue
    dlq: aio_pika.RobustQueue


async def declare_topology(channel: aio_pika.abc.AbstractChannel) -> Topology:
    settings = get_settings()

    main_exchange = await channel.declare_exchange(
        settings.main_exchange, ExchangeType.DIRECT, durable=True
    )
    retry_exchange = await channel.declare_exchange(
        settings.retry_exchange, ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(settings.dlx_exchange, ExchangeType.DIRECT, durable=True)

    main_queue = await channel.declare_queue(
        settings.main_queue,
        durable=True,
        arguments={
            # transient failures dead-letter into the retry exchange (delayed redelivery)
            "x-dead-letter-exchange": settings.retry_exchange,
            "x-dead-letter-routing-key": settings.routing_key,
        },
    )
    await main_queue.bind(main_exchange, routing_key=settings.routing_key)

    retry_queue = await channel.declare_queue(
        settings.retry_queue,
        durable=True,
        arguments={
            # safety cap; the real per-message delay is set via `expiration` on publish
            "x-message-ttl": settings.retry_queue_max_ttl_seconds * 1000,
            # expired retries flow back into the main queue
            "x-dead-letter-exchange": settings.main_exchange,
            "x-dead-letter-routing-key": settings.routing_key,
        },
    )
    await retry_queue.bind(retry_exchange, routing_key=settings.routing_key)

    dlq = await channel.declare_queue(settings.dlq_queue, durable=True)
    await dlq.bind(dlx, routing_key=settings.routing_key)

    return Topology(
        main_exchange=main_exchange,
        retry_exchange=retry_exchange,
        dlx=dlx,
        main_queue=main_queue,
        retry_queue=retry_queue,
        dlq=dlq,
    )


def _message(
    body: bytes | dict,
    headers: dict | None = None,
    delay_seconds: float | None = None,
) -> Message:
    if not isinstance(body, (bytes, bytearray)):
        body = json.dumps(body, separators=(",", ":")).encode("utf-8")
    return Message(
        body=bytes(body),
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,
        headers=headers or {},
        expiration=int(delay_seconds) if delay_seconds else None,
    )


class Publisher:
    """Owns a dedicated connection for publishing, with publisher confirms enabled."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.RobustChannel | None = None
        self._topology: Topology | None = None

    async def connect(self, attempts: int = 20, delay: float = 2.0) -> None:
        last_exc: Exception | None = None
        for _ in range(attempts):
            try:
                self._connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                logger.warning("rabbitmq not reachable yet (%s); retrying...", exc)
                await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc

        self._channel = await self._connection.channel()
        # publisher confirms are enabled by default in aio-pika>=9
        # (channel(publisher_confirms=True)); exchange.publish() awaits and
        # returns the broker confirmation frame.
        self._topology = await declare_topology(self._channel)
        logger.info("publisher connected to rabbitmq")

    def _ensure(self) -> Topology:
        if self._topology is None:
            raise RuntimeError("publisher is not connected")
        return self._topology

    @staticmethod
    def _check_confirmed(result, action: str) -> None:
        if result is not None and type(result).__name__ != "Ack":
            raise RuntimeError(f"broker did not confirm {action}: {result!r}")

    async def publish_job(self, body: bytes | dict, headers: dict | None = None) -> None:
        topology = self._ensure()
        result = await topology.main_exchange.publish(
            _message(body, headers), routing_key=self.settings.routing_key
        )
        self._check_confirmed(result, "job publish")

    async def publish_retry(
        self, body: bytes | dict, headers: dict | None = None, delay_seconds: float = 0
    ) -> None:
        topology = self._ensure()
        result = await topology.retry_exchange.publish(
            _message(body, headers, delay_seconds), routing_key=self.settings.routing_key
        )
        self._check_confirmed(result, "retry publish")

    async def publish_to_dlq(self, body: bytes | dict, headers: dict | None = None) -> None:
        topology = self._ensure()
        result = await topology.dlx.publish(
            _message(body, headers), routing_key=self.settings.routing_key
        )
        self._check_confirmed(result, "dlq publish")

    async def purge_queues(self) -> dict[str, int]:
        """Purge all messages from main / retry / DLQ (demo reset endpoint).

        Returns the per-queue message counts as they were before the purge.
        """
        topology = self._ensure()
        purged = {
            self.settings.main_queue: (await topology.main_queue.purge()).message_count,
            self.settings.retry_queue: (await topology.retry_queue.purge()).message_count,
            self.settings.dlq_queue: (await topology.dlq.purge()).message_count,
        }
        logger.info("queues purged: %s", purged)
        return purged

    async def queue_stats(self) -> dict[str, dict[str, int]]:
        """Live message counts for main / retry / DLQ via PASSIVE declarations.

        Passive declares only assert that the queues exist (with the exact
        arguments they already have) and return the broker's counters — they
        never re-declare or change anything, so there is no PRECONDITION_FAILED
        risk. No database access, which is the point: the dashboard's queue
        panel keeps working (and proves messages are cooling down in the retry
        queue) even while the DB is down.

        Declared on the *underlay* aiormq channel on purpose: a passive
        declare_queue() on the RobustChannel returns the CACHED queue object
        with its stale declaration_result from connect time (all counters 0).
        """
        self._ensure()  # raises a clear error if the publisher is not connected
        underlay = await self._channel.get_underlay_channel()
        stats: dict[str, dict[str, int]] = {}
        for name in (
            self.settings.main_queue,
            self.settings.retry_queue,
            self.settings.dlq_queue,
        ):
            result = await underlay.queue_declare(queue=name, passive=True)
            stats[name] = {
                "messages": result.message_count,
                "consumers": result.consumer_count,
            }
        return stats

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._topology = None
