import json

from confluent_kafka import Consumer, Producer

from .processing import calculate_dew_point, calculate_heat_index

REDPANDA_BROKER = "localhost:19092"

INPUT_TOPIC = "sensor-readings"
OUTPUT_TOPIC = "processed-readings"

CONSUMER_GROUP = "analytics-group"

consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

producer = Producer({
    "bootstrap.servers": REDPANDA_BROKER,
    "client.id": "iot-analytics",
    "enable.idempotence": True,
    "acks": "all"
})

def delivery_report(err, msg):
    """Report whether an event reached redpanda"""

    if err is not None:
        print(f"processed event delivery failed: {err}")
    else:
        print(
            f"Procesed event published to "
            f"{msg.topic()}"
            f"[partition {msg.partition()}]"
            f"offset {msg.offset()}"
        )

def process_event(event: dict) -> dict:
    """Add calculated analytics to the event dict"""

    temperature = event["temperature"]
    humidity = event["humidity"]

    dew_point = calculate_dew_point(temperature, humidity)
    heat_index = calculate_heat_index(temperature, humidity)

    processed_event = {
        **event,
        "dew_point": dew_point,
        "heat_index": heat_index
    }

    return processed_event

def main():
    print("Starting analytic consumer.....")
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Output topic: {OUTPUT_TOPIC}")

    consumer.subscribe([INPUT_TOPIC])

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

                print(
                    f"\nReceived event "
                    f"{event['event_id']} "
                    f"from partition {message.partition()}"
                )

                processed_event = process_event(event)
                print(f"processed event: {processed_event}")

                producer.produce(
                    topic=OUTPUT_TOPIC,
                    key=event["device_id"],
                    value=json.dumps(processed_event),
                    callback=delivery_report
                )

                producer.poll(0)

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                print(f"Invalid event: {e}")
           
    except KeyboardInterrupt:
        print("\nStopping analytics consumer.....")

    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()