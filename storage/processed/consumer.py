import json
from datetime import datetime, timezone

from confluent_kafka import Consumer
from sqlalchemy.exc import IntegrityError

from database import SessionLocal
from models import Analytics

REDPANDA_BROKER = "localhost:19092"
INPUT_TOPIC = "processed-readings"
CONSUMER_GROUP = "analytic-storage-group"

consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest", 
})

def store_processed_readings(event: dict, msg_partition, msg_offset) -> None:
    session = SessionLocal()

    try: 
        readings = Analytics(
            event_id=event["event_id"],
            device_id=event["device_id"],
            temperature=event["temperature"],
            humidity=event["humidity"],
            dew_point=event["dew_point"],
            heat_index=event["heat_index"],
            processed_at=datetime.now(timezone.utc),
        )

        session.add(readings)
        session.commit()

        print(
            f"Stored processed event {event['event_id']} "
            f"from partition {msg_partition} "
            f"offset {msg_offset}"
        )

    except IntegrityError:
    
        session.rollback()        

        print(
            f"Processed event {event['event_id']} "
            "already exists. Skipping duplicate."
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
                print(f"Consumer error: {message.error()}")
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))
                
                store_processed_readings(
                    event,
                    msg_partition=message.partition(),
                    msg_offset=message.offset(),
                )
                
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except (json.JSONDecodeError) as e:
                print(f"Invalid message: {e}")

    except KeyboardInterrupt:
        print("\nStopping processed storage consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()