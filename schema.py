from datetime import datetime, timezone

from pydantic import BaseModel, Field

class SensorReadingScheme(BaseModel):
    event_id: str = Field(min_length=36, max_length=36)

    device_id: str = Field(min_length=1, max_length=50)

    temperature: float = Field(
        ge=-40,
        le=90,
    )

    humidity: float = Field(
        ge=0,
        le=100,
    )

    timestamp: datetime