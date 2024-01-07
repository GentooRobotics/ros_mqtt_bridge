#!/usr/bin/env python3
import rospy
from omegaconf import DictConfig
from .task_type import MQTT2ROSItem, ROS2MQTTItem
from .mqtt_bridge import MQTTBridge
from .ros_bridge import ROSBridge
import yaml
from typing import Dict


def main() -> None:
    rospy.init_node("ros_mqtt_bridge")
    config_path = rospy.get_param("~config_path", "bridge.yaml")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg = DictConfig(cfg)

    ros2mqtt_tasks: Dict[str, ROS2MQTTItem] = {}
    mqtt2ros_tasks: Dict[str, MQTT2ROSItem] = {}

    # ROS Bridge
    ros_bridge = ROSBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)

    # MQTT Bridge
    mqtt_bridge = MQTTBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)

    rate = rospy.Rate(cfg["ros2mqtt"]["loop_rate"])

    while not rospy.is_shutdown():
        mqtt_bridge.run_once()
        rate.sleep()
