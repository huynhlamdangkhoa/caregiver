import paho.mqtt.client as mqtt
import logging
import requests
import time
import os
import threading

# MQTT Broker Configuration
BROKER = "localhost"
PORT = 1883

# MQTT Topics
APP_TOPIC = "home/control"
DEVICE_TOPIC = "home/devices"
DEVICE_TOPICS = {
    "fan": "home/devices/fan",
    "light": "home/devices/light",
    "ac": "home/devices/ac"
}

# ThingSpeak Configuration
THINGSPEAK_CHANNEL_ID = "YOUR_CHANNEL_ID"  # Thay bằng Channel ID của bạn
THINGSPEAK_READ_API_KEY = "YOUR_READ_API_KEY"  # Thay bằng Read API Key của bạn
THINGSPEAK_API_URL = f"https://api.thingspeak.com/channels/{THINGSPEAK_CHANNEL_ID}/feeds.json?api_key={THINGSPEAK_READ_API_KEY}&results=1"

# Configure Logging
logging.basicConfig(
    filename="hub.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker")
        print("Connected to MQTT Broker")
        client.subscribe(APP_TOPIC)
    else:
        logging.error(f"Failed to connect, return code {rc}")
        print(f"Failed to connect, return code {rc}")

# Callback when a message is received
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        logging.info(f"Received message on {msg.topic}: {payload}")
        print(f"Received: {payload}")

        command = payload.split(":")
        if len(command) == 2:
            device, action = command
            topic = DEVICE_TOPICS.get(device)
            if topic:
                client.publish(topic, action)
                logging.info(f"Forwarded '{action}' to {device} on topic {topic}")
                print(f"Forwarded '{action}' to {device} on topic {topic}")
                client.publish(f"{APP_TOPIC}/ack", f"{device}:{action}:success")
            else:
                logging.warning(f"Unknown device: {device}")
                client.publish(f"{APP_TOPIC}/ack", f"{device}:unknown")
        else:
            logging.warning("Invalid message format")
            client.publish(f"{APP_TOPIC}/ack", "error:invalid_format")
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        print(f"Error: {e}")

# Callback when the client disconnects
def on_disconnect(client, userdata, rc):
    logging.warning("Disconnected from MQTT Broker")
    print("Disconnected from MQTT Broker")

# Function to check and process ThingSpeak data
def check_thingspeak_data(client):
    # Reverse mapping dictionaries
    object_map = {1: "light", 2: "fan"}  # Ngược lại từ upload_to_thingspeak
    action_map = {1: "on", 0: "off"}     # Ngược lại từ upload_to_thingspeak

    while True:
        try:
            response = requests.get(THINGSPEAK_API_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("feeds"):
                    latest_feed = data["feeds"][0]
                    # Lấy giá trị từ field1 (device) và field2 (action)
                    device_id = latest_feed.get("field1")
                    action_id = latest_feed.get("field2")

                    if device_id in object_map and action_id in action_map:
                        device = object_map[int(device_id)]
                        action = action_map[int(action_id)]
                        topic = DEVICE_TOPICS.get(device)
                        if topic:
                            client.publish(topic, action)
                            logging.info(f"Sent '{action}' to {device} on topic {topic} from ThingSpeak")
                            print(f"Sent '{action}' to {device} on topic {topic} from ThingSpeak")
                        else:
                            logging.warning(f"Unknown device from ThingSpeak: {device}")
                    else:
                        logging.warning(f"Invalid device or action ID from ThingSpeak: {device_id}, {action_id}")
            else:
                logging.warning(f"Failed to fetch ThingSpeak data, status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching ThingSpeak data: {e}")
            print(f"Error fetching ThingSpeak data: {e}")
        time.sleep(15)  # Đợi 15 giây, theo giới hạn API ThingSpeak

# Setup MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Connect to the broker
try:
    client.connect(BROKER, PORT, 60)
except Exception as e:
    logging.error(f"Could not connect to MQTT Broker: {e}")
    print(f"Could not connect to MQTT Broker: {e}")
    exit(1)

# Start the loop and ThingSpeak checking in separate threads
logging.info("Central Hub is running...")
print("Central Hub is running...")
client.loop_start()
check_thread = threading.Thread(target=check_thingspeak_data, args=(client,), daemon=True)
check_thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logging.info("Central Hub stopped by user")
    print("Central Hub stopped by user")
    client.loop_stop()
    client.disconnect()
