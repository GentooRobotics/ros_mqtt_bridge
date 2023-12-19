# ros
import time
from typing import Any, Tuple
import rospy

# others
import hydra
import json
from omegaconf import DictConfig
from .ros_serializers.message_converter import convert_dictionary_to_ros_message
from functools import partial
from queue import Queue
from .task_type import ros2mqtt_task, mqtt2ros_task
MsgType = type[rospy.Message]


class ROSBridge():
    REQUIRED_MQTT_2_ROS_KEYS = ("ros_topic_name", "ros_type")
    REQUIRED_ROS_2_MQTT_TOPIC_KEYS = ("mqtt_topic_name", "ros_type")
    REQUIRED_SERVICE_KEYS = ("ros_service_name", "ros_type", "mqtt_response_topic_name")
    RATE_BUFFER = 0.05
    def __init__(self, 
                 cfg: DictConfig, 
                 ros2mqtt_tasks: Queue[ros2mqtt_task], 
                 mqtt2ros_tasks: Queue[mqtt2ros_task]
                ):
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks 
        self.mqtt2ros_tasks = mqtt2ros_tasks
        self.ros_publishers: dict[str, rospy.Publisher]= {}
        self.ros_service_clients: dict[str, rospy.ServiceProxy]= {}

        self.create_ros_subscribers()
        self.create_ros_service_clients()

        # ROS client
        if "mqtt2ros" not in self.cfg or "loop_rate" not in self.cfg["mqtt2ros"]:
            self.loop_rate = 20
        else:
            self.loop_rate = self.cfg["mqtt2ros"]["loop_rate"]

        self.sleep_time = 1. / (self.loop_rate + self.RATE_BUFFER) # default loop rate

        self.ros_publisher_available = "mqtt2ros" in self.cfg and ("topic2topic" in self.cfg["mqtt2ros"] and self.cfg["mqtt2ros"]["topic2topic"])

        if self.ros_publisher_available:
            self.publisher_rate = {}
            self.publisher_last_published = {}
            self.create_ros_publishers()
            self.timer = rospy.timer.Timer(rospy.Duration(self.sleep_time), self.timer_callback)

    
    def create_ros_subscribers(self):
        if "ros2mqtt" not in self.cfg or "topic2topic" not in self.cfg["ros2mqtt"] or not self.cfg["ros2mqtt"]["topic2topic"]:
            rospy.logwarn(f"[ROS Bridge] No ROS2MQTT Topic 2 Topic found..")
            return
        # ros2mqtt (topic2topic)
        for ros_topic in self.cfg["ros2mqtt"]["topic2topic"]:
            ros2mqtt: dict[str, str] = self.cfg["ros2mqtt"]["topic2topic"][ros_topic]
            if not all(key in ros2mqtt for key in self.REQUIRED_ROS_2_MQTT_TOPIC_KEYS):
                # get the missing key and print it out
                missing_keys = [key for key in self.REQUIRED_ROS_2_MQTT_TOPIC_KEYS if key not in ros2mqtt]
                rospy.logerr(f"[ROS Bridge] Skipping ROS2MQTT Topic 2 Topic {ros_topic} due to missing keys {missing_keys}")
                raise ValueError(f"Missing keys {missing_keys} in {ros_topic}")
            
            ros_type_name: str = ros2mqtt["ros_type"]
            mqtt_topic: str = ros2mqtt["mqtt_topic_name"]
            converter: str = ros2mqtt.get("converter", "")
            try:
                ros_type = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                rospy.logerr(f"The ros message type {ros_type_name} could not be found")
                raise
            
            rospy.Subscriber(
                name=ros_topic,
                data_class=ros_type,
                callback=partial(self.ros_callback, converter=converter, ros_topic=ros_topic),
                queue_size=10
            )
            rospy.loginfo(f"[ROS Bridge] bridge <-  [ROS][topic]   {ros_topic}")
            rospy.loginfo(f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_topic}")
            
    
    def ros_callback(self, msg, converter, ros_topic):
        rospy.loginfo(f"[MQTT Bridge] Received [ROS][topic]: {ros_topic}")
        mqtt_topic = self.cfg["ros2mqtt"]["topic2topic"][ros_topic]["mqtt_topic_name"]
        self.ros2mqtt_tasks.put((mqtt_topic, msg, time.time(), converter))
    
    def create_ros_publishers(self):
        # mqtt2ros (topic2topic)
        if "mqtt2ros" not in self.cfg or "topic2topic" not in self.cfg["ros2mqtt"] or not self.cfg["ros2mqtt"]["topic2topic"]:
            rospy.logwarn(f"[ROS Bridge] No MQTT2ROS Topic 2 Topic found..")
            return
        
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            topic2topic: dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][mqtt_topic]
            if not all(key in topic2topic for key in self.REQUIRED_MQTT_2_ROS_KEYS):
                # get the missing key and print it out
                missing_keys = [key for key in self.REQUIRED_MQTT_2_ROS_KEYS if key not in topic2topic]
                rospy.logerr(f"[ROS Bridge] Skipping MQTT2ROS Topic 2 Topic {mqtt_topic} due to missing keys {missing_keys}")
                raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")

            ros_topic: str = topic2topic["ros_topic_name"]
            ros_type_name: str = topic2topic["ros_type"] 
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                rospy.logerr(f"The ros message type {ros_type_name} could not be found")
                raise
            self.ros_publishers[mqtt_topic] = rospy.Publisher(
                name=ros_topic, data_class=ros_type, queue_size=1
            )
            if "rate" in topic2topic:
                given_rate = topic2topic["rate"]
                if given_rate > self.loop_rate:
                    rospy.logwarn(f"[MQTT Bridge] rate given {topic2topic['rate']} is higher than loop rate, setting rate to loop rate {self.loop_rate}")
                    self.publisher_rate[mqtt_topic] = self.loop_rate + self.RATE_BUFFER
                else:
                    self.publisher_rate[mqtt_topic] = given_rate + self.RATE_BUFFER
            else:
                self.publisher_rate[mqtt_topic] = self.loop_rate + self.RATE_BUFFER
            self.publisher_last_published[mqtt_topic] = 0

            rospy.loginfo(f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            rospy.loginfo(f"[ROS Bridge]        ->  [ROS][topic]   {ros_topic}")
    
    def create_ros_service_clients(self):
        if "mqtt2ros" not in self.cfg or "topic2service" not in self.cfg["ros2mqtt"] or not self.cfg["ros2mqtt"]["topic2service"]:
            rospy.logwarn(f"[ROS Bridge] No ROS2MQTT Topic 2 Service found..")
            return
        # mqtt2ros (topic2service)
        for mqtt_topic in self.cfg["mqtt2ros"]["topic2service"]:
            topic2service: dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][mqtt_topic]
            
            if not all(key in topic2service for key in self.REQUIRED_SERVICE_KEYS):
                # get the missing key and print it out
                missing_keys = [key for key in self.REQUIRED_SERVICE_KEYS if key not in topic2service]
                rospy.logerr(f"[ROS Bridge] Skipping MQTT2ROS Topic 2 service {mqtt_topic} due to missing keys {missing_keys}")
                raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")

            ros_service: str = topic2service["ros_service_name"]
            mqtt_response_topic: str = topic2service["mqtt_response_topic_name"]
            ros_type_name: str = topic2service["ros_type"] 
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                rospy.logerr(f"The ros message type {ros_type_name} could not be found")
                raise
            self.ros_service_clients[mqtt_topic] = rospy.ServiceProxy(
                name=ros_service, service_class=ros_type
            )
            rospy.loginfo(f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            rospy.loginfo(f"[ROS Bridge]        <-> [ROS][service] {ros_service}")
            rospy.loginfo(f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}")
    
    def timer_callback(self, _):
        # print('[ROS Bridge] Timer callback triggered.')
        # print(f'[ROS Bridge] MQTT2ROS Tasks: {self.mqtt2ros_tasks.qsize()}')

        if self.mqtt2ros_tasks.empty():
            rospy.logdebug(f"[ROS Bridge] No Pending Tasks..")
            return
        
        current_time = time.time()
        for _ in range(self.mqtt2ros_tasks.qsize()):

            mqtt_receive_topic_name, payload, command = self.mqtt2ros_tasks.get()

            if (command == "topic2topic"):
                if (current_time - self.publisher_last_published[mqtt_receive_topic_name] < 1. / self.publisher_rate[mqtt_receive_topic_name]):
                    continue

                ros_msg_typename: str = self.cfg["mqtt2ros"]["topic2topic"][mqtt_receive_topic_name]["ros_type"]
                self.ros_publishers[mqtt_receive_topic_name].publish(convert_dictionary_to_ros_message(hydra.utils.get_class(ros_msg_typename), json.loads(payload)))
                self.publisher_last_published[mqtt_receive_topic_name] = current_time
            
            elif (command == "topic2service"):
                try:
                    self.ros_service_clients[mqtt_receive_topic_name].wait_for_service(timeout=timeout)
                except rospy.exceptions.ROSException:
                    rospy.logerr(f"Service {ros_service} timed out")
                    return
                
                topic2service: dict[str, Any] = self.cfg["mqtt2ros"]["topic2service"][mqtt_receive_topic_name]
                ros_service: str = topic2service["ros_service_name"]
                ros_srv_typename: str = topic2service["ros_type"]
                timeout: float = topic2service.get("timeout", 1.0)
                
                try:
                    request = convert_dictionary_to_ros_message(hydra.utils.get_class(ros_srv_typename+"Request"), json.loads(payload))
                except AttributeError as e:
                    rospy.logerr(e)
                    return
                
                rospy.logdebug(f"[ROS Bridge] Calling [ROS][service]: {ros_service}")
                response = self.ros_service_clients[mqtt_receive_topic_name].call(request)
                
                mqtt_response_topic_name: str = topic2service["mqtt_response_topic_name"]
                
                rospy.logdebug(f"Received Response [ROS][service]: {ros_service}")
                self.ros2mqtt_tasks.put((mqtt_response_topic_name, response, time.time(), "primitive"))