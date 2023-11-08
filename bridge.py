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
from threading import Thread

from mqtt_bridge import MQTTBridge 
from ros_bridge import ROSBridge

@hydra.main(version_base=None, config_path="./configs", config_name="bridge")
def main(cfg: DictConfig) -> None:
    # stupid, why need this????
    time.sleep(30)

    rclpy.init()
    
    ros2mqtt_tasks = Queue(maxsize=cfg["ros2mqtt"]["queue_size"])
    mqtt2ros_tasks = Queue(maxsize=cfg["ros2mqtt"]["queue_size"])

    # MQTT Bridge Thread
    mqtt_bridge = MQTTBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)
    mqtt_bridge_thread = Thread(target=mqtt_bridge.run)

    # ROS Bridge Thread
    ros_bridge = ROSBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)
    ros_bridge_thread = Thread(target=rclpy.spin, args=(ros_bridge,))

    mqtt_bridge_thread.start()
    ros_bridge_thread.start()
    mqtt_bridge_thread.join()
    ros_bridge_thread.join()


if __name__ == "__main__":
    main()