import time
import paho.mqtt.client as mqtt
import json

# Define the MQTT broker and topic
topic = "number"

# Define a function to publish a message to the MQTT topic
def publish_message(client, topic, message):
    client.publish(topic, json.dumps(message))

# Create an MQTT client
client = mqtt.Client()

# Connect to the MQTT broker
client.connect(host="localhost", port=1883, keepalive=60)

# Your message to publish as a string
# message_to_publish = {"header": {"stamp": {"sec": 0} }, "pose": {"position": {"x": 10}}}
# message_to_publish = {"data": }

# Publish the message to the topic
current_time = time.time()
while(True):
    publish_message(client, "number", {"data": 1})
    time.sleep(1/20 - (time.time() - current_time))
    current_time = time.time()
# Disconnect from the MQTT broker
client.disconnect()
