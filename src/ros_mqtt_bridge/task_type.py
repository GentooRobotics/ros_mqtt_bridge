
from collections import namedtuple

ros2mqtt_task = namedtuple("ros2mqtt_task", ["mqtt_topic", "msg", "time" ,"converter"])
mqtt2ros_task = namedtuple("mqtt2ros_task", ["mqtt_topic", "payload", "command"])