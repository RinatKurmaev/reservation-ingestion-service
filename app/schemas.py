"""API request/response models and validation rules."""

from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError, model_validator

from .models import JobState


class ReservationIn(BaseModel):
    checkin: date
    checkout: date
    num_guests: int = Field(ge=1)
    guest_names: list[str] = Field(min_length=1)
    callback_url: HttpUrl | None = None
    # Fault injection for scenario demos: not stored, not part of the idempotency
    # key (the worker fails the job on the first delivery attempt only for
    # "transient"; "permanent" fails on every replay).
    simulate_error: Literal["transient", "permanent"] | None = None

    @model_validator(mode="after")
    def _validate_rules(self) -> "ReservationIn":
        if self.checkout <= self.checkin:
            raise ValueError("checkout must be after checkin")
        if any(not name.strip() for name in self.guest_names):
            raise ValueError("guest_names must not contain blank names")
        return self


class ReservationAccepted(BaseModel):
    job_id: UUID
    # "pending" (row committed before publish) or "queued" (queue-first
    # admission during a DB outage: the message carries the full job spec and
    # the worker materializes the row once the DB is back — see task.md §1).
    state: str


class BulkItemOut(BaseModel):
    """Per-item admission result for a bulk submission (index is 0-based)."""

    index: int
    accepted: bool
    job_id: UUID | None = None
    detail: str | None = None


class ReservationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    job_id: UUID | None = None
    checkin: date
    checkout: date
    num_guests: int
    guest_names: list[str]
    created_at: datetime


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    state: JobState
    retry_count: int
    last_error: str | None = None
    reservation_id: UUID | None = None
    created_at: datetime
    updated_at: datetime
