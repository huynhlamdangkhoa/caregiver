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
THINGSPEAK_CHANNEL_ID = "2978875"
THINGSPEAK_READ_API_KEY = "DNT5K8NXRSCJJCPG"
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
    object_map = {1: "light", 2: "fan", 3: "ac"}  
    action_map = {1: "on", 0: "off"}  
    ac_action_map = {1: "power", 2: "temp_up", 3: "temp_down"}  

    while True:
        try:
            print(f"Checking ThingSpeak data at {time.strftime('%H:%M:%S')}")
            response = requests.get(THINGSPEAK_API_URL, timeout=10)
            print(f"ThingSpeak response status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Raw data from ThingSpeak: {data}")
                if data.get("feeds"):
                    latest_feed = data["feeds"][0]
                    print(f"Latest feed: {latest_feed}")
                    device_id = latest_feed.get("field1")
                    action_id = latest_feed.get("field2")
                    ac_action_id = latest_feed.get("field3")  

                    print(f"Extracted - Device ID: {device_id}, Action ID: {action_id}, AC Action ID: {ac_action_id}")

                    if device_id and action_id and device_id.isdigit() and action_id.isdigit():
                        device = object_map.get(int(device_id))
                        action = action_map.get(int(action_id))
                        print(f"Mapped - Device: {device}, Action: {action}")
                        if device and action and device in ["light", "fan"]:  # Chỉ áp dụng on/off cho light/fan
                            topic = DEVICE_TOPICS.get(device)
                            if topic:
                                result = client.publish(topic, action)
                                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                                    logging.info(f"Sent '{action}' to {device} on topic {topic} from ThingSpeak")
                                    print(f"Sent '{action}' to {device} on topic {topic} from ThingSpeak")
                                    client.publish(f"{APP_TOPIC}/ack", f"{device}:{action}:success")
                                else:
                                    logging.error(f"Failed to publish to {topic}, return code: {result.rc}")
                                    print(f"Failed to publish to {topic}, return code: {result.rc}")
                            else:
                                logging.warning(f"Unknown device from ThingSpeak: {device}")
                                print(f"Unknown device from ThingSpeak: {device}")
                        elif device == "ac" and ac_action_id and ac_action_id.isdigit():
                            ac_action = ac_action_map.get(int(ac_action_id))
                            if ac_action:
                                topic = DEVICE_TOPICS.get(device)
                                if topic:
                                    result = client.publish(topic, ac_action)
                                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                                        logging.info(f"Sent '{ac_action}' to {device} on topic {topic} from ThingSpeak")
                                        print(f"Sent '{ac_action}' to {device} on topic {topic} from ThingSpeak")
                                        client.publish(f"{APP_TOPIC}/ack", f"{device}:{ac_action}:success")
                                    else:
                                        logging.error(f"Failed to publish to {topic}, return code: {result.rc}")
                                        print(f"Failed to publish to {topic}, return code: {result.rc}")
                                else:
                                    logging.warning(f"Unknown device from ThingSpeak: {device}")
                                    print(f"Unknown device from ThingSpeak: {device}")
                        else:
                            logging.warning(f"Invalid mapping for device_id: {device_id}, action_id: {action_id}, ac_action_id: {ac_action_id}")
                            print(f"Invalid mapping for device_id: {device_id}, action_id: {action_id}, ac_action_id: {ac_action_id}")
                    else:
                        logging.warning(f"Missing or invalid field1 or field2 in feed: {latest_feed}")
                        print(f"Missing or invalid field1 or field2 in feed: {latest_feed}")
                else:
                    logging.warning("No feeds data in ThingSpeak response")
                    print("No feeds data in ThingSpeak response")
            else:
                logging.warning(f"Failed to fetch ThingSpeak data, status code: {response.status_code}")
                print(f"Failed to fetch ThingSpeak data, status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching ThingSpeak data: {e}")
            print(f"Error fetching ThingSpeak data: {e}")
        time.sleep(15)

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
logging.info("Starting ThingSpeak check thread...")
print("Starting ThingSpeak check thread...")
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
