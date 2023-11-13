# ros
from typing import Tuple
import rclpy
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.client import Client as rclClient, SrvType, SrvTypeRequest, SrvTypeResponse
from rclpy.publisher import MsgType, Publisher as rclPublisher

# others
import hydra
import json
import paho.mqtt.client as mqtt
from omegaconf import DictConfig
from ros_serializers.message_converter import convert_dictionary_to_ros_message
from functools import partial
from queue import Queue

class ROSBridge(Node):

    def __init__(self, 
                 cfg: DictConfig, 
                 ros2mqtt_tasks: Queue[Tuple[str, str, MsgType, str]], 
                 mqtt2ros_tasks: Queue[Tuple[str, str, str]]
                ):
        super().__init__("ros_mqtt_bridge")
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks 
        self.mqtt2ros_tasks = mqtt2ros_tasks
        self.ros_publishers: dict[str, rclPublisher]= {}
        self.ros_service_clients: dict[str, rclClient]= {}

        self.create_ros_publishers()
        self.create_ros_subscribers()
        self.create_ros_service_clients()
        self.timer = self.create_timer(self.cfg["mqtt2ros"]["loop_rate"], self.timer_callback)
    
    def create_ros_subscribers(self):
        # ros2mqtt (topic2topic)
        for ros_topic in self.cfg["ros2mqtt"]["topic2topic"].keys():
            ros2mqtt: str = self.cfg["ros2mqtt"]["topic2topic"][ros_topic]
            ros_type_name: str = ros2mqtt["ros_type"]
            mqtt_topic: str = ros2mqtt["to"]
            converter: str = ros2mqtt.get("converter", "")
            try:
                ros_type: SrvType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                self.get_logger().error(f"The ros message type {ros_type_name} could not be found")
                raise

            self.create_subscription(
                ros_type,
                ros_topic,
                partial(self.ros_callback, converter=converter, ros_topic=ros_topic),
                qos_profile=rclpy.qos.qos_profile_sensor_data,
            )
            print(f"[ROS Bridge] bridge <-  [ROS][topic]   {ros_topic}")
            print(f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_topic}")
            
    
    def ros_callback(self, msg, converter, ros_topic):
        # print(f"[MQTT Bridge] Received [ROS][topic]: {ros_topic}")
        self.ros2mqtt_tasks.put(("topic2topic", ros_topic, msg, converter))
    
    def create_ros_publishers(self):
        # mqtt2ros (topic2topic)
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            topic2topic: dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]
            ros_topic: str = topic2topic["to"]
            ros_type_name: str = topic2topic["ros_type"] 
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                self.get_logger().error(f"The ros message type {ros_type_name} could not be found")
                raise
            self.ros_publishers[mqtt_topic] = self.create_publisher(
                ros_type, ros_topic, qos_profile=rclpy.qos.qos_profile_sensor_data
            )

            print(f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            print(f"[ROS Bridge]        ->  [ROS][topic]   {ros_topic}")
    
    def create_ros_service_clients(self):
        self.cb_group = ReentrantCallbackGroup()
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            topic2service: dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
            ros_service: str = topic2service["to"]
            mqtt_response_topic: str = topic2service["response"]
            ros_type_name: str = topic2service["ros_type"] 
            try:
                ros_type: SrvType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                self.get_logger().error(f"The ros message type {ros_type_name} could not be found")
                raise
            self.ros_service_clients[mqtt_topic] = self.create_client(
                ros_type, ros_service, callback_group=self.cb_group
            )
            self.ros_service_clients[mqtt_topic].wait_for_service()
            print(f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            print(f"[ROS Bridge]        <-> [ROS][service] {ros_service}")
            print(f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}")
    
    async def timer_callback(self):
        # print('[ROS Bridge] Timer callback triggered.')
        # print(f'[ROS Bridge] MQTT2ROS Tasks: {self.mqtt2ros_tasks.qsize()}')

        if not self.mqtt2ros_tasks.empty():
            cmd, mqtt_topic, payload = self.mqtt2ros_tasks.get()

            if (cmd == "topic2topic"):
                ros_msg_typename: str = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]["ros_type"]
                self.ros_publishers[mqtt_topic].publish(convert_dictionary_to_ros_message(hydra.utils.get_class(ros_msg_typename), json.loads(payload)))
            
            elif (cmd == "topic2service"):
                # print('[ROS Bridge] Topic 2 Service Triggered..')
                topic2service = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
                ros_service: str = topic2service["to"]
                ros_srv_typename: str = topic2service["ros_type"]
                mqtt_response_topic: str = topic2service["response"]
                try:
                    req: SrvTypeRequest = convert_dictionary_to_ros_message(hydra.utils.get_class(ros_srv_typename).Request, json.loads(payload))
                except AttributeError as e:
                    self.get_logger().error(e)
                    return
                print(f"[ROS Bridge] Called [ROS][service]: {ros_service}")
                response: SrvTypeResponse = await self.ros_service_clients[mqtt_topic].call_async(req)
                print(f"Received Response [ROS][service]: {ros_service}")
                self.ros2mqtt_tasks.put(("topic2service", mqtt_topic, response, "primitive"))
            print('[ROS Bridge] Dequeued..')