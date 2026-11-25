"""Webhook delivery: retried with backoff, never blocks the reservation worker."""

import asyncio
import logging
import random

import httpx

from .config import get_settings

logger = logging.getLogger(__name__)

# Keep strong references to in-flight deliveries so they are not garbage-collected.
_background_tasks: set[asyncio.Task] = set()


def schedule_webhook(client: httpx.AsyncClient, url: str, payload: dict) -> None:
    task = asyncio.create_task(deliver(client, url, payload))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def deliver(client: httpx.AsyncClient, url: str, payload: dict) -> None:
    settings = get_settings()
    for attempt in range(1, settings.webhook_max_attempts + 1):
        try:
            response = await client.post(url, json=payload, timeout=settings.webhook_timeout_seconds)
            if response.status_code < 400:
                logger.info(
                    "webhook delivered job=%s attempt=%d status=%d",
                    payload.get("job_id"), attempt, response.status_code,
                )
                return
            logger.warning(
                "webhook attempt %d/%d failed job=%s status=%d",
                attempt, settings.webhook_max_attempts, payload.get("job_id"), response.status_code,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "webhook attempt %d/%d errored job=%s: %s",
                attempt, settings.webhook_max_attempts, payload.get("job_id"), exc,
            )
        if attempt < settings.webhook_max_attempts:
            delay = min(2.0 * 2 ** (attempt - 1), 60.0)
            await asyncio.sleep(delay + random.uniform(0, 2.0))

    # Delivery exhausted: dropped, but logged with the full payload for manual replay.
    logger.error(
        "webhook delivery exhausted after %d attempts job=%s url=%s payload=%s",
        settings.webhook_max_attempts, payload.get("job_id"), url, payload,
    )
