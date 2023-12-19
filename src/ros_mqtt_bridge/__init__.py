#!/usr/bin/env python3
import rospy
from omegaconf import DictConfig
from queue import Queue
from typing import Tuple

from .task_type import ros2mqtt_task, mqtt2ros_task
from .mqtt_bridge import MQTTBridge 
from .ros_bridge import ROSBridge
import yaml
MsgType = type[rospy.Message]



def main() -> None:

    rospy.init_node("ros_mqtt_bridge")
    config_path = rospy.get_param("~config_path", "bridge.yaml")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg = DictConfig(cfg)
    default_queue_size = 10
    ros2mqtt_queue_size = cfg["ros2mqtt"]["queue_size"] if "ros2mqtt" in cfg and "queue_size" in cfg["ros2mqtt"] else default_queue_size
    mqtt2ros_queue_size = cfg["mqtt2ros"]["queue_size"] if "mqtt2ros" in cfg and "queue_size" in cfg["mqtt2ros"] else default_queue_size

    ros2mqtt_tasks: Queue[ros2mqtt_task] = Queue(maxsize=ros2mqtt_queue_size)
    mqtt2ros_tasks: Queue[mqtt2ros_task] = Queue(maxsize=mqtt2ros_queue_size)

    # ROS Bridge
    ros_bridge = ROSBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)

    # MQTT Bridge
    mqtt_bridge = MQTTBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)

    rate = rospy.Rate(cfg["ros2mqtt"]["loop_rate"])

    while not rospy.is_shutdown():
        mqtt_bridge.run_once()
        rate.sleep()