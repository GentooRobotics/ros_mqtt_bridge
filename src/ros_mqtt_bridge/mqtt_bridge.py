from typing import Any
import time
import json
import paho.mqtt.client as mqtt
from omegaconf import DictConfig
from . import ros_serializers
from .ros_serializers.message_converter import convert_ros_message_to_dictionary
from .task_type import MQTT2ROSItem, ROS2MQTTItem, CommunicationType
from .field_checker import (
    check_for_ros_2_mqtt_keys,
    check_for_service_keys,
    check_for_mqtt_2_ros_keys,
)
from typing import Dict

import rospy
import threading


class MQTTBridge:
    def __init__(
        self,
        cfg: DictConfig,
        ros2mqtt_tasks: Dict[str, ROS2MQTTItem],
        mqtt2ros_tasks: Dict[str, MQTT2ROSItem],
    ):
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks
        self.mqtt2ros_tasks = mqtt2ros_tasks
        self.shutdown_flag = threading.Event()
        self.publisher_rate = {}

        # mqtt client
        if "ros2mqtt" not in self.cfg or "loop_rate" not in self.cfg["ros2mqtt"]:
            self.loop_rate = 20
        else:
            self.loop_rate = self.cfg["ros2mqtt"]["loop_rate"]
        self.sleep_time = 1.0 / (self.loop_rate)  # default loop rate

        self.topic2topic_available = "ros2mqtt" in self.cfg and (
                "topic2topic" in self.cfg["ros2mqtt"]
                and self.cfg["ros2mqtt"]["topic2topic"]
            )
        
        self.topic2service_available = "ros2mqtt" in self.cfg and (
                "topic2service" in self.cfg["ros2mqtt"]
                and self.cfg["ros2mqtt"]["topic2service"]
            )
        if self.topic2topic_available:
            for ros_topic in self.cfg["ros2mqtt"]["topic2topic"]:
                ros2mqtt: Dict[str, str] = self.cfg["ros2mqtt"]["topic2topic"][
                    ros_topic
                ]
                check_for_ros_2_mqtt_keys("MQTT", ros_topic, ros2mqtt)
                mqtt_topic_name: str = ros2mqtt["mqtt_topic_name"]
                self.initialize_publish_rate(ros2mqtt, mqtt_topic_name)

        if self.topic2service_available:
            for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"]:
                mqtt2ros: Dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][
                    mqtt_topic
                ]
                check_for_service_keys("MQTT", mqtt_topic, mqtt2ros)
                mqtt_response_topic_name: str = mqtt2ros["mqtt_response_topic_name"]
                self.initialize_publish_rate(mqtt2ros, mqtt_response_topic_name)

        self.connected = False
        self.mqtt_client = self.get_mqtt_client()
        self.mqtt_client.loop_start()

    def initialize_publish_rate(self, cfg: dict, publisher_key: str):
        if "rate" in cfg:
            given_rate = cfg["rate"]
            if given_rate > self.loop_rate:
                rospy.logwarn(
                    f"[MQTT Bridge] rate given {cfg['rate']} is higher than loop rate, setting rate to loop rate {self.loop_rate}"
                )
                self.publisher_rate[publisher_key] = 1 / (
                    1 / self.loop_rate - self.sleep_time / 2
                )
            else:
                self.publisher_rate[publisher_key] = 1 / (
                    1 / given_rate - self.sleep_time / 2
                )
        else:
            self.publisher_rate[publisher_key] = 1 / (
                1 / self.loop_rate - self.sleep_time / 2
            )

    def get_mqtt_client(self):
        client = mqtt.Client()
        while not rospy.is_shutdown():
            try:
                client.connect(
                    host=self.cfg["mqtt_broker"]["host"],
                    port=self.cfg["mqtt_broker"]["port"],
                    keepalive=self.cfg["mqtt_broker"]["keepalive"],
                )
                client.on_connect = self.on_mqtt_connect
                client.on_disconnect = self.on_mqtt_disconnect
                client.on_message = self.on_mqtt_message
                rospy.loginfo("[MQTT Bridge] Connected to MQTT Broker")
                return client
            except KeyError:
                rospy.logerr("Missing key in config file")
                raise
            except Exception:
                rospy.logwarn(
                    "[MQTT Bridge] Unable to connect to MQTT Broker, retrying in 1s"
                )
                time.sleep(1)

    def on_mqtt_connect(
        self, mqtt_client: mqtt.Client, userdata: Any, flags: dict, rc: int
    ):
        rospy.loginfo("[MQTT Bridge] Connected successfully to MQTT Broker")
        self.subscribe_to_mqtt_topics()
        self.connected = True

    def on_mqtt_disconnect(self, mqtt_client: mqtt.Client, userdata: Any, rc: int):
        rospy.logwarn("[MQTT Bridge] Disconnected from MQTT Broker")
        self.connected = False

    def on_mqtt_message(
        self, mqtt_client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ):
        rospy.logdebug("[MQTT Bridge] MQTT Broker Callback")
        if msg.topic in self.cfg["mqtt2ros"]["topic2topic"]:
            self.mqtt2ros_tasks[msg.topic].payload = msg.payload
            self.mqtt2ros_tasks[msg.topic].command = "topic2topic"
            self.mqtt2ros_tasks[msg.topic].last_received_time = time.time()
            rospy.logdebug(f"[MQTT Bridge] MQTT2ROS Topic 2 Topic {msg.topic}")

        elif msg.topic in self.cfg["mqtt2ros"]["topic2service"]:
            self.mqtt2ros_tasks[msg.topic].payload = msg.payload
            self.mqtt2ros_tasks[msg.topic].command = "topic2service"
            self.mqtt2ros_tasks[msg.topic].last_received_time = time.time()
            rospy.logdebug(f"[MQTT Bridge] MQTT2ROS Topic 2 Service {msg.topic}")

    def subscribe_to_mqtt_topics(self):
        if "mqtt2ros" not in self.cfg:
            return

        # mqtt2ros (topic2topic)
        if (
            "topic2topic" in self.cfg["mqtt2ros"]
            and self.cfg["mqtt2ros"]["topic2topic"]
        ):
            for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"]:
                topic2topic: Dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][
                    mqtt_topic
                ]
                check_for_mqtt_2_ros_keys("MQTT", mqtt_topic, topic2topic)

                ros_topic: str = topic2topic["ros_topic_name"]
                self.mqtt_client.subscribe(mqtt_topic)

                rospy.loginfo(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
                rospy.loginfo(f"[MQTT Bridge]        ->  [ROS][topic]   {ros_topic}")

        # mqtt2ros (topic2service)
        if (
            "topic2service" in self.cfg["mqtt2ros"]
            and self.cfg["mqtt2ros"]["topic2service"]
        ):
            for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"]:
                topic2service: Dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][
                    mqtt_topic
                ]
                check_for_service_keys("MQTT", mqtt_topic, topic2service)

                ros_service: str = topic2service["ros_service_name"]
                self.mqtt_client.subscribe(mqtt_topic)
                mqtt_response_topic: str = topic2service["mqtt_response_topic_name"]

                rospy.loginfo(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
                rospy.loginfo(f"[MQTT Bridge]        <-> [ROS][service] {ros_service}")
                rospy.loginfo(
                    f"[MQTT Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}"
                )

    def run_once(self):
        if not self.topic2topic_available and not self.topic2service_available:
            rospy.logwarn_once(f"[MQTT Bridge]: MQTT Publisher not available..")
            return

        if not self.connected:
            rospy.logwarn_throttle(
                5, f"[MQTT Bridge]: Waiting to connect to MQTT Broker.."
            )
            return

        for mqtt_topic, ros2mqtt_item in self.ros2mqtt_tasks.items():
            if not ros2mqtt_item.msg:
                continue
            if (
                ros2mqtt_item.last_received_time < ros2mqtt_item.last_published_time
            ):  # no new message
                continue

            if (ros2mqtt_item.commmunication_type == CommunicationType.TOPIC2TOPIC) and (
                time.time() - ros2mqtt_item.last_published_time
                < 1.0 / self.publisher_rate[mqtt_topic]
            ):
                continue

            if hasattr(ros_serializers, ros2mqtt_item.converter):
                convert = getattr(ros_serializers, ros2mqtt_item.converter)
            else:
                convert = convert_ros_message_to_dictionary

            self.mqtt_client.publish(mqtt_topic, json.dumps(convert(ros2mqtt_item.msg)))
            ros2mqtt_item.last_published_time = time.time()
