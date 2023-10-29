# ros
import rclpy
from cv_bridge import CvBridge
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.logging import set_logger_level, LoggingSeverity

# others
import base64
import hydra
import json
import cv2
import paho.mqtt.client as mqtt
from omegaconf import DictConfig, OmegaConf
import ros_serializers


class MQTTBridge(Node):
    def __init__(self, cfg):
        super().__init__("mqtt_bridge")
        set_logger_level(
            name="", level=getattr(LoggingSeverity, cfg["logging_severity"])
        )

        self.cfg = cfg
        self.mqtt_client = self.get_mqtt_client()
        self.mqtt_client.loop_start()
        self.ros_subscribers = {}
        self.ros_service_clients = {}

        # ros2mqtt (topic2topic)
        for ros_topic in cfg["ros2mqtt"]["topic2topic"].keys():
            ros2mqtt = cfg["ros2mqtt"]["topic2topic"][ros_topic]
            ros_type = ros2mqtt["ros_type"]
            mqtt_topic = ros2mqtt["to"]
            converter = ros2mqtt["converter"]

            self.create_subscription(
                hydra.utils.get_class(ros2mqtt["ros_type"]),
                ros_topic,
                lambda msg, ros_topic=ros_topic, topic=mqtt_topic, converter=converter: self.ros_callback(
                    msg, topic, converter, ros_topic
                ),
                qos_profile=rclpy.qos.qos_profile_sensor_data,
            )
            self.get_logger().info(f"bridge <-  [ROS][topic]   {ros_topic}")
            self.get_logger().info(f"       ->  [MQTT][topic]  {mqtt_topic}")

        # mqtt2ros (topic2service)
        self.cb_group = ReentrantCallbackGroup()
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            topic2service = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
            ros_service = topic2service["to"]
            mqtt_response_topic = topic2service["response"]
            ros_type = hydra.utils.get_class(topic2service["ros_type"])

            self.mqtt_client.subscribe(mqtt_topic)
            self.ros_service_clients[mqtt_topic] = self.create_client(
                ros_type, ros_service, callback_group=self.cb_group
            )

            self.get_logger().info(f"bridge <-  [MQTT][topic]  {mqtt_topic}")
            self.get_logger().info(f"       <-> [ROS][service] {ros_service}")
            self.get_logger().info(f"       ->  [MQTT][topic]  {mqtt_response_topic}")

    def ros_callback(self, msg, mqtt_topic, converter, ros_topic):
        self.get_logger().debug(f"Received [ROS][topic]: {ros_topic}")
        self.mqtt_client.publish(
            mqtt_topic, json.dumps(getattr(ros_serializers, converter)(msg))
        )

        self.get_logger().debug(f"Published [MQTT][topic]: {mqtt_topic}")

    def get_mqtt_client(self):
        client = mqtt.Client()
        client.on_connect = self.on_mqtt_connect
        client.on_message = self.on_mqtt_message
        # client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASSWORD)
        client.connect(
            host=self.cfg["mqtt_broker"]["host"],
            port=self.cfg["mqtt_broker"]["port"],
            keepalive=self.cfg["mqtt_broker"]["keepalive"],
        )
        return client

    def on_mqtt_connect(self, mqtt_client, userdata, flags, rc):
        self.get_logger().info("Connected successfully to MQTT Broker")

    def on_mqtt_message(self, mqtt_client, userdata, msg):
        self.get_logger().debug(f"Received [MQTT][topic]: {msg.topic}")
        if msg.topic in self.cfg["mqtt2ros"]["topic2service"].keys():
            decoded_data = json.loads(msg.payload)

            topic2service = self.cfg["mqtt2ros"]["topic2service"][msg.topic]
            ros_service = topic2service["to"]
            mqtt_response_topic = topic2service["response"]

            req = hydra.utils.get_class(topic2service["ros_type"]).Request()
            for key, value in decoded_data.items():
                setattr(req, key, value)

            self.get_logger().debug(f"Called [ROS][service]: {ros_service}")
            response = self.ros_service_clients[msg.topic].call(req)
            self.get_logger().debug(f"Received Response [ROS][service]: {ros_service}")
            self.mqtt_client.publish(
                mqtt_response_topic,
                json.dumps(ros_serializers.primitive_serializer(response)),
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
