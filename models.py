from datetime import datetime

from sqlalchemy import Float, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base

class SensorReading(Base):
    __tablename__ = "sensor_readings"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    event_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        unique=True,
        index=True,
    )

    device_id: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True,
    )

    temperature: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    humidity: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    processed_reading: Mapped["Analytics | None"] = relationship(
        back_populates="sensor_reading",
        uselist=False,
    )

    alerts: Mapped[list["Alert"]] = relationship(
        back_populates="sensor_reading"
    )


class Analytics(Base):
    __tablename__ = "processed_readings"

    event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("sensor_readings.event_id"),
        primary_key=True,
    )

    device_id: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True
    )

    temperature: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    humidity: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    dew_point: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    heat_index: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    sensor_reading: Mapped["SensorReading"] = relationship(
        back_populates="processed_readings"
    )


class Alert(Base):
    __tablename__ = "alerts"

    alert_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("sensor_readings.event_id"),
        nullable=False,
        index=True,
    )

    alert_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    severity: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    value: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    sensor_reading: Mapped["SensorReading"] = relationship(
        back_populates="alerts"
    )