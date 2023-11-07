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

class MQTTBridge(Node):
    def __init__(self, cfg: DictConfig):
        super().__init__("mqtt_bridge")
        set_logger_level(
            name="", level=getattr(LoggingSeverity, cfg["logging_severity"])
        )

        self.cfg = cfg
        self.mqtt_client = self.get_mqtt_client()
        self.mqtt_client.loop_start()
        self.ros_subscribers = {}
        self.ros_topic_publishers: dict[str, rclPublisher]= {}
        self.ros_service_clients: dict[str, rclClient]= {}

        # ros2mqtt (topic2topic)
        for ros_topic in cfg["ros2mqtt"]["topic2topic"].keys():
            ros2mqtt: str = cfg["ros2mqtt"]["topic2topic"][ros_topic]
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
                partial(self.ros_callback, mqtt_topic=mqtt_topic, converter=converter, ros_topic=ros_topic),
                qos_profile=rclpy.qos.qos_profile_sensor_data,
            )
            self.get_logger().info(f"bridge <-  [ROS][topic]   {ros_topic}")
            self.get_logger().info(f"       ->  [MQTT][topic]  {mqtt_topic}")

        for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            topic2topic: dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]
            ros_topic: str = topic2topic["to"]
            ros_type_name: str = topic2topic["ros_type"] 
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                self.get_logger().error(f"The ros message type {ros_type_name} could not be found")
                raise
            self.mqtt_client.subscribe(mqtt_topic)
            self.ros_topic_publishers[mqtt_topic] = self.create_publisher(
                ros_type, ros_topic, qos_profile=rclpy.qos.qos_profile_sensor_data
            )

            self.get_logger().info(f"bridge <-  [MQTT][topic]  {mqtt_topic}")
            self.get_logger().info(f"       ->  [ROS][topic] {ros_topic}")

        # mqtt2ros (topic2service)
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
            self.mqtt_client.subscribe(mqtt_topic)
            self.ros_service_clients[mqtt_topic] = self.create_client(
                ros_type, ros_service, callback_group=self.cb_group
            )

            self.get_logger().info(f"bridge <-  [MQTT][topic]  {mqtt_topic}")
            self.get_logger().info(f"       <-> [ROS][service] {ros_service}")
            self.get_logger().info(f"       ->  [MQTT][topic]  {mqtt_response_topic}")
        


    def ros_callback(self, msg, mqtt_topic, converter, ros_topic):
        self.get_logger().debug(f"Received [ROS][topic]: {ros_topic}")

        if hasattr(ros_serializers, converter):
            convert = getattr(ros_serializers, converter)
        else:
            convert = convert_ros_message_to_dictionary
        self.mqtt_client.publish(
            mqtt_topic, json.dumps(convert(msg))
        )

        self.get_logger().debug(f"Published [MQTT][topic]: {mqtt_topic}")

    def get_mqtt_client(self):
        client = mqtt.Client()
        # client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASSWORD)

        while True: 
            try:
                client.connect(
                        host=self.cfg["mqtt_broker"]["host"],
                        port=self.cfg["mqtt_broker"]["port"],
                        keepalive=self.cfg["mqtt_broker"]["keepalive"],
                    )
                client.on_connect = self.on_mqtt_connect
                client.on_message = self.on_mqtt_message
                self.get_logger().info("Connected to MQTT Broker")
                return client
            except:
                self.get_logger().warn("Unable to connect to MQTT Broker, retrying in 1s")
                time.sleep(1)
    
    def on_mqtt_connect(self, mqtt_client: mqtt.Client, userdata: Any, flags: dict, rc: int):
        self.get_logger().info("Connected successfully to MQTT Broker")

    def on_mqtt_message(self, mqtt_client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        self.get_logger().debug(f"Received [MQTT][topic]: {msg.topic}")
        if msg.topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            decoded_data = json.loads(msg.payload)

            topic2topic: str = self.cfg["mqtt2ros"]["topic2topic"][msg.topic]
            ros_topic: str = topic2topic["to"]
            ros_msg_typename: str = topic2topic["ros_type"]
            try:
                ros_msg: MsgType = convert_dictionary_to_ros_message(hydra.utils.get_class(ros_msg_typename), decoded_data)
            except AttributeError as e:
                self.get_logger().error(e)
                return

            self.ros_topic_publishers[msg.topic].publish(ros_msg) 
            self.get_logger().debug(f"Publish [ROS][topic]: {ros_topic}")
            
        elif msg.topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            decoded_data = json.loads(msg.payload)

            topic2service: str = self.cfg["mqtt2ros"]["topic2service"][msg.topic]
            ros_service: str = topic2service["to"]
            mqtt_response_topic: str = topic2service["response"]

            try:
                req: SrvTypeRequest = convert_dictionary_to_ros_message(hydra.utils.get_class(topic2service["ros_type"]).Request, decoded_data)
            except AttributeError as e:
                self.get_logger().error(e)
                return
            self.get_logger().debug(f"Called [ROS][service]: {ros_service}")
            response = self.ros_service_clients[msg.topic].call(req) # since it is running on another thread blocking is allowed (different thread than the ros spin)
            self.get_logger().debug(f"Received Response [ROS][service]: {ros_service}")
            self.mqtt_client.publish(
                mqtt_response_topic,
                json.dumps(convert_ros_message_to_dictionary(response)),
            )
            self.get_logger().debug(f"Published [MQTT][topic]: {mqtt_response_topic}")


@hydra.main(version_base=None, config_path="./configs", config_name="bridge")
def main(cfg: DictConfig) -> None:
    rclpy.init()
    node = MQTTBridge(cfg)
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
