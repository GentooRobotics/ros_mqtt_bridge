# Deprecated: This file is deprecated
import rclpy
from rclpy.qos import QoSProfile
from rclpy.node import Node
from rclpy.publisher import Publisher
from cv_bridge import CvBridge

import hydra
import base64
import time
import json
import cv2
import paho.mqtt.client as mqtt

rclpy.init()
timeout = 2  # seconds


def get_config():
    with hydra.initialize(version_base=None, config_path="./configs"):
        cfg = hydra.compose(config_name="bridge")
        return cfg


def get_mqtt_client(cfg):
    client = mqtt.Client()
    client.connect(
        host=cfg["mqtt_broker"]["host"],
        port=cfg["mqtt_broker"]["port"],
        keepalive=cfg["mqtt_broker"]["keepalive"],
    )
    return client


def test_number_and_type_ros_subscribers():
    cfg = get_config()
    node = rclpy.create_node("test_number_and_type_ros_subscribers")
    time.sleep(timeout)  # node takes some time to scan available topics
    topic_names_and_types = node.get_topic_names_and_types()

    assert len(topic_names_and_types) >= len(cfg["ros2mqtt"]["topic2topic"])

    for ros_topic in cfg["ros2mqtt"]["topic2topic"]:
        topic2topic = cfg["ros2mqtt"]["topic2topic"][ros_topic]
        ros_type = topic2topic["ros_type"].replace(".", "/")

        assert (ros_topic, [ros_type]) in topic_names_and_types


def test_primitive_converter():
    cfg = get_config()
    mqtt_client = get_mqtt_client(cfg)
    mqtt_client.loop_start()
    node = rclpy.create_node("test_primitive_converter")

    topic = "/chatter"
    topic2topic = cfg["ros2mqtt"]["topic2topic"][topic]
    mqtt_topic = topic2topic["mqtt_topic_name"]
    ros_type = topic2topic["ros_type"]
    converter = topic2topic["converter"]
    assert converter == "primitive_serializer"

    data = "Hello World"

    def on_message(mqtt_client, userdata, msg):
        assert msg.topic == mqtt_topic

        decoded_message = json.loads(msg.payload)
        assert decoded_message["data"] == data

    mqtt_client.on_message = on_message
    mqtt_client.subscribe(mqtt_topic)
    publisher = node.create_publisher(
        hydra.utils.get_class(ros_type), topic, QoSProfile(depth=10)
    )

    msg = hydra.utils.get_class(ros_type)()

    for i in range(10):
        msg.data = data
        publisher.publish(msg)


def test_image_converter():
    cfg = get_config()
    mqtt_client = get_mqtt_client(cfg)
    mqtt_client.loop_start()
    node = rclpy.create_node("test_primitive_converter")

    topic = "/test_map_with_laser"
    topic2topic = cfg["ros2mqtt"]["topic2topic"][topic]
    mqtt_topic = topic2topic["mqtt_topic_name"]
    ros_type = topic2topic["ros_type"]
    converter = topic2topic["converter"]
    assert converter == "image_serializer"

    image = cv2.imread("./src/mqtt_bridge/tests/assets/gray.png")
    _, buffer = cv2.imencode(".png", image)
    image_data = base64.b64encode(buffer).decode("utf-8")
    encoded_image_data = f"data:image/png;base64,{image_data}"

    def on_message(mqtt_client, userdata, msg):
        assert msg.topic == mqtt_topic

        decoded_message = json.loads(msg.payload)
        assert decoded_message["data"] == encoded_image_data

    mqtt_client.on_message = on_message
    mqtt_client.subscribe(mqtt_topic)
    publisher = node.create_publisher(
        hydra.utils.get_class(ros_type), topic, QoSProfile(depth=10)
    )

    for i in range(10):
        bridge = CvBridge()
        msg = bridge.cv2_to_imgmsg(
            image,
            encoding="bgr8",
        )
        publisher.publish(msg)
        time.sleep(0.1)
