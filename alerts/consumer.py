import json
from confluent_kafka import Consumer, Producer

REDPANDA_BROKER = "localhost:19092"

INPUT_TOPIC = "sensor-readings"
OUTPUT_TOPIC = "sensor-alerts"

CONSUMER_GROUP = "alert-group"

consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
})

producer = Producer({
    "bootstrap.servers": REDPANDA_BROKER,
    "client.id": "iot-alerts",
    "enable.idempotence": True,
})

def create_alert(
        event:dict,
        alert_type:str,
        severity:str,
        value:str,
        threshold:float,
) -> dict:

    return {
        "event_id": event["event_id"],
        "device_id": event["device_id"],
        "alert_type": alert_type,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "timestamp": event["timestamp"],
    }

def check_alerts(event: dict) -> list[dict]:

    alerts = []

    temperature = event["temperature"]
    humidity = event["humidity"]

    # High temperature
    if 33 < temperature <= 38:
        alerts.append(
            create_alert(
                event,
                alert_type="High Temperature",
                severity="Mild warming.",
                value=temperature,
                threshold=33
            )
        )

    if 38 < temperature <= 43:
        alerts.append(
                    create_alert(
                        event,
                        alert_type="High Temperature",
                        severity="High warming!",
                        value=temperature,
                        threshold=38
                    )
                )

    if temperature > 43:
        alerts.append(
                    create_alert(
                        event,
                        alert_type="High Temperature",
                        severity="Critical warming!!",
                        value=temperature,
                        threshold=43
                    )
                )

    if temperature < 5:
        alerts.append(
            create_alert(
                event,
                alert_type="Low Temperature",
                severity="Critical!!",
                value=temperature,
                threshold=5,
            )
        )

    # High humidity
    if humidity > 80:
        alerts.append(
            create_alert(
                event,
                alert_type="HIGH_HUMIDITY",
                severity="MEDIUM",
                value=humidity,
                threshold=80,
            )
        )

    return alerts

def publish_alert(alert: dict) -> None:

    producer.produce(
        topic=OUTPUT_TOPIC,
        key=alert["device_id"],
        value=json.dumps(alert).encode("utf-8"),
    )

    producer.flush()

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
            
            event = json.loads(
                message.value().decode("utf-8")
            )

            alerts = check_alerts(event)

            for alert in alerts:

                publish_alert(alert)

                print(
                    f"ALERT: {alert['alert_type']} "
                    f"for {alert['device_id']} "
                    f"(value={alert['value']}, "
                    f"threshold={alert['threshold']})"
                )

            # Only acknowledge the Kafka message after
            # alert processing has completed.
            consumer.commit(
                message=message,
                asynchronous=False,
            )

    except KeyboardInterrupt:

        print("\nStopping alert consumer...")

    finally:

        consumer.close()


if __name__ == "__main__":
    main()