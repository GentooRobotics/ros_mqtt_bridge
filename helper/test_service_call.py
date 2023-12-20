import time
import paho.mqtt.client as mqtt
import json


global_time = time.time()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("set_bool_response")  # Subscribe to the desired MQTT topic
    else:
        print(f"Failed to connect, return code: {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, message):
    global global_time
    # print(f"Received message on topic {message.topic}: {message.payload.decode()}")
    print(f"Frequency: {1/(time.time() - global_time)}")
    global_time = time.time()


# Define the MQTT broker and topic
topic = "set_bool_request"


# Define a function to publish a message to the MQTT topic
def publish_message(client, topic, message):
    client.publish(topic, json.dumps(message))


# Create an MQTT client
client = mqtt.Client()

# Set up the callbacks
client.on_connect = on_connect
client.on_message = on_message


# Connect to the MQTT broker
client.connect(host="localhost", port=1883, keepalive=60)

client.loop_start()
# Publish the message to the topic
current_time = time.time()
while True:
    publish_message(client, "set_bool_request", {"data": True})
    time.sleep(0.1 - (time.time() - current_time))
    current_time = time.time()
# Disconnect from the MQTT broker
client.disconnect()
