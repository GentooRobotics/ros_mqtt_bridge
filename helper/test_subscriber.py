import paho.mqtt.client as mqtt
import time

# Callback when the client connects to the broker
global_time = time.time()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("chatter")  # Subscribe to the desired MQTT topic
    else:
        print(f"Failed to connect, return code: {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, message):
    global global_time
    print(f"Received message on topic {message.topic}: {message.payload.decode()}")
    print(f"Frequency: {1/(time.time() - global_time)}")
    global_time = time.time()


# Create an MQTT client
client = mqtt.Client()

# Set up the callbacks
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(
    "localhost", 1883, 60
)  # Replace with your MQTT broker's address and port

# Start the MQTT loop to listen for messages
client.loop_forever()
