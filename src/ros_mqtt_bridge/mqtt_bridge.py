from typing import Any, Tuple
import time
import json
import paho.mqtt.client as mqtt
from omegaconf import DictConfig
from . import ros_serializers
from .ros_serializers.message_converter import convert_ros_message_to_dictionary
from .task_type import ros2mqtt_task, mqtt2ros_task
from queue import Queue
from .killable_timer import KillableTimer

import rospy
MsgType = type[rospy.Message]
import threading

class MQTTBridge:
    # mqtt2ros keys
    REQUIRED_MQTT_2_ROS_KEYS = ("ros_topic_name", "ros_type")
    REQUIRED_ROS_2_MQTT_TOPIC_KEYS = ("mqtt_topic_name", "ros_type")
    REQUIRED_SERVICE_KEYS = ("ros_service_name", "ros_type", "mqtt_response_topic_name")
    RATE_BUFFER_PERCENTAGE = 0.05
    def __init__(self, 
                 cfg: DictConfig, 
                 ros2mqtt_tasks: Queue[ros2mqtt_task], 
                 mqtt2ros_tasks: Queue[mqtt2ros_task]
                ):
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks 
        self.mqtt2ros_tasks = mqtt2ros_tasks
        self.shutdown_flag = threading.Event()
        self.last_published = 0
        self.publisher_rate = {}
        self.publisher_last_published = {}
        
        # mqtt client
        if "ros2mqtt" not in self.cfg or "loop_rate" not in self.cfg["ros2mqtt"]:
            self.loop_rate = 20
        else:
            self.loop_rate = self.cfg["ros2mqtt"]["loop_rate"]
        
        self.mqtt_publisher_available = "ros2mqtt" in self.cfg and (("topic2topic" in self.cfg["ros2mqtt"] and self.cfg["ros2mqtt"]["topic2topic"]) or ("topic2service" in self.cfg["ros2mqtt"] and self.cfg["ros2mqtt"]["topic2service"]))
        if self.mqtt_publisher_available:
            for ros_topic in self.cfg["ros2mqtt"]["topic2topic"]:
                ros2mqtt: dict[str, str] = self.cfg["ros2mqtt"]["topic2topic"][ros_topic]
                if not all(key in ros2mqtt for key in self.REQUIRED_ROS_2_MQTT_TOPIC_KEYS):
                    missing_keys = [key for key in self.REQUIRED_ROS_2_MQTT_TOPIC_KEYS if key not in ros2mqtt]
                    rospy.logerr(f"[MQTT Bridge] Skipping ROS2MQTT Topic 2 Topic {ros_topic} due to missing keys {missing_keys}")
                    raise ValueError(f"Missing keys {missing_keys} in {ros_topic}")
                mqtt_topic_name: str = ros2mqtt["mqtt_topic_name"]
                if "rate" in ros2mqtt:
                    given_rate = ros2mqtt["rate"]
                    if given_rate > self.loop_rate:
                        rospy.logwarn(f"[MQTT Bridge] rate given {ros2mqtt['rate']} is higher than loop rate, setting rate to loop rate {self.loop_rate}")
                        self.publisher_rate[mqtt_topic_name] = self.loop_rate * (1 + self.RATE_BUFFER_PERCENTAGE)
                    else:
                        self.publisher_rate[mqtt_topic_name] = given_rate * (1 + self.RATE_BUFFER_PERCENTAGE)
                else:
                    self.publisher_rate[mqtt_topic_name] = self.loop_rate * (1 + self.RATE_BUFFER_PERCENTAGE)
                self.publisher_last_published[mqtt_topic_name] = 0
        self.mqtt_client = self.get_mqtt_client()
        self.connected = False
        self.mqtt_client.loop_start()

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
                rospy.logwarn("[MQTT Bridge] Unable to connect to MQTT Broker, retrying in 1s")
                time.sleep(1)
    
    def on_mqtt_connect(self, mqtt_client: mqtt.Client, userdata: Any, flags: dict, rc: int):
        rospy.loginfo("[MQTT Bridge] Connected successfully to MQTT Broker")
        self.subscribe_to_mqtt_topics()
        self.connected = True
    
    def on_mqtt_disconnect(self, mqtt_client: mqtt.Client, userdata: Any, rc: int):
        rospy.logwarn("[MQTT Bridge] Disconnected from MQTT Broker")
        self.connected = False

    def on_mqtt_message(self, mqtt_client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        rospy.logdebug("[MQTT Bridge] MQTT Broker Callback")
        if msg.topic in self.cfg["mqtt2ros"]["topic2topic"]:
            self.mqtt2ros_tasks.put((msg.topic, msg.payload, "topic2topic"))
            rospy.logdebug(f"[MQTT Bridge] Enqueued MQTT2ROS Topic 2 Topic")
        elif msg.topic in self.cfg["mqtt2ros"]["topic2service"]:
            self.mqtt2ros_tasks.put((msg.topic, msg.payload, "topic2service"))
            rospy.logdebug(f"[MQTT Bridge] Enqueued MQTT2ROS Topic 2 Service")
    
    def subscribe_to_mqtt_topics(self):
        if "mqtt2ros" not in self.cfg:
            return
        
        # mqtt2ros (topic2topic)
        if "topic2topic" in self.cfg["mqtt2ros"] and self.cfg["mqtt2ros"]["topic2topic"]:
            
            for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
                topic2topic: dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]
                if not all(key in topic2topic for key in self.REQUIRED_MQTT_2_ROS_KEYS):
                    # get the missing key and print it out
                    missing_keys = [key for key in self.REQUIRED_MQTT_2_ROS_KEYS if key not in topic2topic]
                    rospy.logerr(f"[MQTT Bridge] Skipping MQTT2ROS Topic 2 Topic {mqtt_topic} due to missing keys {missing_keys}")
                    raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")
                ros_topic: str = topic2topic["ros_topic_name"]
                self.mqtt_client.subscribe(mqtt_topic)

                rospy.logdebug(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
                rospy.logdebug(f"[MQTT Bridge]        ->  [ROS][topic]   {ros_topic}")

        # mqtt2ros (topic2service)
        if "topic2service" not in self.cfg["mqtt2ros"] or not self.cfg["mqtt2ros"]["topic2service"]:
            for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"].keys():
                topic2service: dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
                if not all(key in topic2service for key in self.REQUIRED_SERVICE_KEYS):
                    # get the missing key and print it out
                    missing_keys = [key for key in self.REQUIRED_SERVICE_KEYS if key not in topic2service]
                    rospy.logerr(f"[MQTT Bridge] Skipping MQTT2ROS Topic 2 service {mqtt_topic} due to missing keys {missing_keys}")
                    raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")
                
                ros_service: str = topic2service["ros_service_name"]
                mqtt_response_topic: str = topic2service["response"]
                self.mqtt_client.subscribe(mqtt_topic)

                rospy.logdebug(f"[MQTT Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
                rospy.logdebug(f"[MQTT Bridge]        <-> [ROS][service] {ros_service}")
                rospy.logdebug(f"[MQTT Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}")

    def run_once(self):
        if (not self.mqtt_publisher_available):
            rospy.logdebug(f"[MQTT Bridge]: MQTT Publisher not available..")
            return
        
        if (not self.connected):
            rospy.logdebug(f"[MQTT Bridge]: Waiting to connect to MQTT Broker..")
            return
        
        # print(f"[MQTT Bridge] Pending Tasks: {self.ros2mqtt_tasks.qsize()}")
        if self.ros2mqtt_tasks.empty():
            rospy.logdebug(f"[MQTT Bridge]: No Pending Tasks..")
            return
            # print(f"[MQTT Bridge] Processing..")

        for _ in range(self.ros2mqtt_tasks.qsize()):
            mqtt_topic, msg, receive_time, converter = self.ros2mqtt_tasks.get()

            if (receive_time - self.publisher_last_published[mqtt_topic] < 1. / self.publisher_rate[mqtt_topic]):
                continue
            print(receive_time - self.publisher_last_published[mqtt_topic])
            if hasattr(ros_serializers, converter):
                convert = getattr(ros_serializers, converter)
            else:
                convert = convert_ros_message_to_dictionary

            self.mqtt_client.publish(mqtt_topic, json.dumps(convert(msg)))
            self.publisher_last_published[mqtt_topic] = receive_time
        self.last_published = receive_time

