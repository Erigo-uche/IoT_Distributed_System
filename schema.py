from datetime import datetime, timezone

from pydantic import BaseModel, Field

class SensorReadingSchema(BaseModel):
    event_id: str = Field(min_length=36, max_length=36)

    device_id: str = Field(min_length=1, max_length=20)

    temperature: float = Field(
        ge=-40,
        le=90,
    )

    humidity: float = Field(
        ge=0,
        le=100,
    )

    timestamp: datetime


class ProcessedReadingSchema(BaseModel):
    event_id: str = Field(
        min_length=36,
        max_length=36,
    )

    device_id: str = Field(
        min_length=1,
        max_length=20,
    )

    temperature: float = Field(
        ge=-40,
        le=90,
    )

    humidity: float = Field(
        ge=0,
        le=100,
    )

    dew_point: float

    heat_index: float

    processed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )


class AlertSchema(BaseModel):
    alert_id: int | None = None

    event_id: str = Field(
        min_length=36,
        max_length=36,
    )

    device_id: str = Field(
        min_length=1,
        max_length=20,
    )

    alert_type: str = Field(
        min_length=1,
        max_length=20,
    )

    severity: str = Field(
        min_length=1,
        max_length=20,
    )

    value: float

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )