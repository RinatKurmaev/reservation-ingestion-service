"""API request/response models and validation rules."""

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

from .models import JobState


class ReservationIn(BaseModel):
    checkin: date
    checkout: date
    num_guests: int = Field(ge=1)
    guest_names: list[str] = Field(min_length=1)
    callback_url: HttpUrl | None = None

    @model_validator(mode="after")
    def _validate_rules(self) -> "ReservationIn":
        if self.checkout <= self.checkin:
            raise ValueError("checkout must be after checkin")
        if any(not name.strip() for name in self.guest_names):
            raise ValueError("guest_names must not contain blank names")
        return self


class ReservationAccepted(BaseModel):
    job_id: UUID
    state: JobState


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
