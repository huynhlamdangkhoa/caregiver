import paho.mqtt.client as mqtt #Thư viện này dùng để tạo client giao tiếp qua giao thức MQTT.
import logging #Import module logging để ghi lại thông tin, cảnh báo, lỗi vào file log (theo dõi hoạt động của chương trình).

# MQTT Broker Configuration
BROKER = "localhost"  # BROKER: địa chỉ của MQTT Broker. Ở đây là "localhost" tức là broker chạy trên cùng máy. Replace with your broker's IP if not local
PORT = 1883  # Default MQTT port

#MQTT Topics
APP_TOPIC = "home/control"       # Topic hận điều khiển từ app
DEVICE_TOPIC = "home/devices"    # General device topic
DEVICE_TOPICS = {                # Specific topics for devices
    "fan": "home/devices/fan",
    "light": "home/devices/light",
    "ac": "home/devices/ac"
}

# Configure Logging
logging.basicConfig(
    filename="hub.log", #Ghi log vào file hub.log.
    level=logging.INFO, #level=INFO: chỉ ghi những log cấp độ từ INFO trở lên (bao gồm WARNING, ERROR).
    format="%(asctime)s - %(levelname)s - %(message)s" #định dạng log bao gồm thời gian, cấp độ, và nội dung.
)

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc): #Hàm callback khi kết nối tới broker
    if rc == 0: #
        logging.info("Connected to MQTT Broker")
        print("Connected to MQTT Broker")
        # Subscribe to the control topic
        client.subscribe(APP_TOPIC)
    else:
        logging.error(f"Failed to connect, return code {rc}")
        print(f"Failed to connect, return code {rc}")

# Callback when a message is received
def on_message(client, userdata, msg): #Hàm được gọi mỗi khi client nhận được một tin nhắn trên topic đã đăng ký.
    try:
        payload = msg.payload.decode() #Giải mã nội dung tin nhắn từ bytes sang string.
        logging.info(f"Received message on {msg.topic}: {payload}")
        print(f"Received: {payload}") #Ghi log và in nội dung tin nhắn nhận được.

        # Parse the message
        command = payload.split(":")
        if len(command) == 2:
            device, action = command  #Giả sử định dạng lệnh là device:action (VD: fan:on), hàm này chia ra làm 2 phần: device và action.
            topic = DEVICE_TOPICS.get(device) #Truy xuất topic thiết bị từ từ điển DEVICE_TOPICS.
            if topic:
                # Forward the command to the specific device
                client.publish(topic, action)
                logging.info(f"Forwarded '{action}' to {device} on topic {topic}")
                print(f"Forwarded '{action}' to {device} on topic {topic}")
                # Acknowledge to the app
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

# Setup MQTT Client
client = mqtt.Client() #Tạo một MQTT client mới.

client.on_connect = on_connect #Gán các hàm xử lý tương ứng khi client kết nối, nhận tin nhắn, hoặc bị ngắt.
client.on_message = on_message 
client.on_disconnect = on_disconnect

# Connect to the broker
try:
    client.connect(BROKER, PORT, 60) #Cố gắng kết nối đến broker tại địa chỉ và cổng đã chỉ định. Timeout = 60s.
except Exception as e:
    logging.error(f"Could not connect to MQTT Broker: {e}")
    print(f"Could not connect to MQTT Broker: {e}")
    exit(1)

# Start the loop
logging.info("Central Hub is running...")
print("Central Hub is running...")
client.loop_forever() #loop_forever() sẽ giữ chương trình chạy mãi và xử lý các sự kiện MQTT liên tục (nhận/gửi tin nhắn...).
