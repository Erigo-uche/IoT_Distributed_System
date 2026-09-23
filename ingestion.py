import json
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from schema import SensorReadingSchema

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/sensors/readings"

REDPANDA_BROKER = "localhost:19092"
REDPANDA_TOPIC = "sensor-readings"

producer = Producer({
    "bootstrap.servers": REDPANDA_BROKER,
    "client.id": "iot-ingestion",
    "enable.idempotence": True,
})

def delivery_report(err, msg):
    """Called by the Kafka/Redpanda producer after delivery succeeds or fails."""
    if err is not None:
        print(f"Redpanda delivery failed: {err}")
    else:
        print(
            f"Published to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )


def on_connect(client, userdata, flags, reason_code, properties):
    """function ran on connection"""
    if reason_code == 0:
        print(f"Connection to MQTT broker: {reason_code}")
        
        client.subscribe(MQTT_TOPIC)
        
        print(f"Subscribed to {MQTT_TOPIC}")
    else:
        print(f"MQTT connection failed: {reason_code}")


def on_message(client, userdata, message):
    """function ran on incoming message"""
    print(f"\nMessage received from {message.topic}.")

    try:
        data = json.loads(message.payload.decode())
    except json.JSONDecodeError:
        print("Invalid JSON received.")

    try:
        validated_data = SensorReadingSchema.model_validate(data)
    except ValueError as e:
        print(f"Validation error: {e}!")
        return

    event = validated_data.model_dump(mode="json")
    print(f"Validated event: {event}")

    producer.produce(
        topic=REDPANDA_TOPIC,
        key=event["device_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )

    producer.poll(0)

def main():
    print("Starting ingestion service...")
    print(f"Redpanda broker: {REDPANDA_BROKER}")
    print(f"Redpanda topic: {REDPANDA_TOPIC}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    print("Connecting to MQTT broker....")
    client.connect(MQTT_BROKER, MQTT_PORT)

    try:
        client.loop_forever()
    finally:
        producer.flush()


if __name__ == "__main__":
    main()