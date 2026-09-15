import json
import random
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/sensors/sensor-001"

DEVICE_ID = "sensor-001"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.connect(MQTT_BROKER, MQTT_PORT)

while True:
    temperature = round(random.uniform(20, 35), 2)
    humidity = round(random.uniform(40, 80), 2)

    reading = {
        "event_id": str(uuid.uuid4()),
        "device_id": DEVICE_ID,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    payload = json.dumps(reading)

    client.publish(MQTT_TOPIC, payload)

    print(f"Published: {payload}")

    time.sleep(5)
    