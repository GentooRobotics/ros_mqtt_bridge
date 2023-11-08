# ros
from typing import Any
import rclpy
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.logging import set_logger_level, LoggingSeverity
from rclpy.client import Client as rclClient, SrvType, SrvTypeRequest, SrvTypeResponse
from rclpy.publisher import MsgType, Publisher as rclPublisher

# others
import hydra
import json
import paho.mqtt.client as mqtt
from omegaconf import DictConfig
import ros_serializers
from ros_serializers.message_converter import convert_dictionary_to_ros_message, convert_ros_message_to_dictionary
from functools import partial
import time
from queue import Queue

class MQTTBridge:

    def __init__(self, cfg: DictConfig, ros2mqtt_tasks, mqtt2ros_tasks):
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks 
        self.mqtt2ros_tasks = mqtt2ros_tasks

        # mqtt client
        self.sleep_time = 1. / self.cfg["ros2mqtt"]["loop_rate"]
        self.mqtt_client = self.get_mqtt_client()
        self.subscribe_to_mqtt_topics()

    def get_mqtt_client(self):
        client = mqtt.Client()
        while True: 
            try:
                client.connect(
                        host=self.cfg["mqtt_broker"]["host"],
                        port=self.cfg["mqtt_broker"]["port"],
                        keepalive=self.cfg["mqtt_broker"]["keepalive"],
                    )
                client.on_connect = self.on_mqtt_connect
                client.on_message = self.on_mqtt_message
                print("[MQTT Bridge] Connected to MQTT Broker")
                return client
            except:
                print("[MQTT Bridge] Unable to connect to MQTT Broker, retrying in 1s")
                time.sleep(1)
    
    def on_mqtt_connect(self, mqtt_client: mqtt.Client, userdata: Any, flags: dict, rc: int):
        print("[MQTT Bridge] Connected successfully to MQTT Broker")

    def on_mqtt_message(self, mqtt_client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        print("[MQTT Bridge] MQTT Broker Callback")
        if msg.topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            self.mqtt2ros_tasks.put(("topic2topic", msg.topic, msg.payload))
            print(f"[MQTT Bridge] Enqueued MQTT2ROS Topic 2 Topic")
        elif msg.topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            self.mqtt2ros_tasks.put(("topic2service", msg.topic, msg.payload))
            print(f"[MQTT Bridge] Enqueued MQTT2ROS Topic 2 Service")
    
    def subscribe_to_mqtt_topics(self):
        # mqtt2ros (topic2topic)
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            topic2topic: dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]
            ros_topic: str = topic2topic["to"]
            ros_type_name: str = topic2topic["ros_type"] 
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                print(f"The ros message type {ros_type_name} could not be found")
                raise
            self.mqtt_client.subscribe(mqtt_topic)

            print(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            print(f"[MQTT Bridge]        ->  [ROS][topic]   {ros_topic}")

        # mqtt2ros (topic2service)
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            topic2service: dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
            ros_service: str = topic2service["to"]
            mqtt_response_topic: str = topic2service["response"]
            ros_type_name: str = topic2service["ros_type"] 
            self.mqtt_client.subscribe(mqtt_topic)

            print(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            print(f"[MQTT Bridge]        <-> [ROS][service] {ros_service}")
            print(f"[MQTT Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}")
    
    def run(self):
        self.mqtt_client.loop_start()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.loop_start()

        while True:
            time.sleep(self.sleep_time)
            print(f"[MQTT Bridge] Pending Tasks: {self.ros2mqtt_tasks.qsize()}")
            if not self.ros2mqtt_tasks.empty():
                print(f"[MQTT Bridge] Processing..")
                cmd, ros_topic, msg, converter = self.ros2mqtt_tasks.get()

                if (cmd == "topic2topic"):
                    if hasattr(ros_serializers, converter):
                        convert = getattr(ros_serializers, converter)
                    else:
                        convert = convert_ros_message_to_dictionary

                    self.mqtt_client.publish(
                        self.cfg["ros2mqtt"][cmd][ros_topic]["to"], json.dumps(convert(msg)))
               
                elif (cmd == "topic2service"):
                    mqtt_topic = ros_topic
                    if hasattr(ros_serializers, converter):
                        convert = getattr(ros_serializers, converter)
                    else:
                        convert = convert_ros_message_to_dictionary
                    
                    print(f"!!! {mqtt_topic}")
                    self.mqtt_client.publish(
                    self.cfg["mqtt2ros"][cmd][mqtt_topic]["response"], json.dumps(convert(msg)))

# @hydra.main(version_base=None, config_path="./configs", config_name="bridge")
# def main(cfg: DictConfig) -> None:
#     ros2mqtt_tasks = Queue(maxsize=10)
#     mqtt2ros_tasks = Queue(maxsize=10)
#     mqtt_bridge = MQTTBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)
#     mqtt_bridge.run()

# if __name__ == "__main__":
#     main()