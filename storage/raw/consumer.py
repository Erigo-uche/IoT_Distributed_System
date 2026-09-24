import json
from datetime import datetime, timezone

from confluent_kafka import Consumer
from sqlalchemy.exc import IntegrityError

from models import SensorReading
from database import SessionLocal

REDPANDA_BROKER = "localhost:19092"
INPUT_TOPIC = "sensor-readings"
CONSUMER_GROUP = "raw-storage-group"

consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

def store_sensor_readings(event: dict, message_partition, message_offset) -> None:
    session = SessionLocal()

    try:
        readings = SensorReading(
            event_id=event["event_id"],
            device_id=event["device_id"],
            temperature=event["temperature"],
            humidity=event["humidity"],
            timestamp=datetime.fromisoformat(event["timestamp"]),
            received_at=datetime.now(timezone.utc).isoformat(),
        )

        session.add(readings)
        session.commit()

        print(
            f"Stored raw event {event["event_id"]} from {event["device_id"]} "
            f"from partition {message_partition} "
            f"offset {message_offset}"
        )

    except IntegrityError:
        session.rollback()

        print(
            f"Event {event["event_id"]} already exists"
            "skipping duplicate."
        )

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


def main():
    consumer.subscribe([INPUT_TOPIC])

    print(
        f"Listening to {INPUT_TOPIC} "
        f"with group {CONSUMER_GROUP}..."
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"consumer error: {message.error}")
                continue

            try: 
                event = json.loads(
                    message.value().decode("utf-8")
                )

                store_sensor_readings(
                    event,
                    message_partition=message.partition(),
                    message_offset=message.offset(),
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except (json.JSONDecodeError) as e:
                print(f"Invalid message: {e}")
    except KeyboardInterrupt:
        print("\nStopping raw storage consumer....")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
