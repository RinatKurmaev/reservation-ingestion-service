"""Application settings (12-factor, env-driven)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Infrastructure
    database_url: str = "postgresql+asyncpg://reservations:reservations@db:5432/reservations"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    # Queue topology
    routing_key: str = "reservations"
    main_exchange: str = "reservations.ex"
    main_queue: str = "reservations.jobs"
    retry_exchange: str = "reservations.retry.ex"
    retry_queue: str = "reservations.retry"
    dlx_exchange: str = "reservations.dlx"
    dlq_queue: str = "reservations.dlq"
    # Safety cap for the retry queue; the real delay is set per-message on publish.
    retry_queue_max_ttl_seconds: int = 600

    # Consumer / retry policy
    prefetch_count: int = 5
    max_retries: int = 5
    backoff_base_seconds: float = 2.0
    backoff_cap_seconds: float = 60.0

    # Reaper: jobs stuck in `processing` longer than the lease are requeued.
    lease_timeout_seconds: int = 300
    reaper_interval_seconds: int = 60

    # Webhook delivery
    webhook_max_attempts: int = 5
    webhook_timeout_seconds: float = 10.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
