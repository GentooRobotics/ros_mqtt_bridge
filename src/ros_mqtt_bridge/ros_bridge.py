# ros
import time
from typing import Any, Dict, Optional
import rospy

# others
import hydra
import json
from omegaconf import DictConfig
from .ros_serializers.message_converter import convert_dictionary_to_ros_message
from functools import partial
from .task_type import MQTT2ROSItem, ROS2MQTTItem, CommunicationType

MsgType = rospy.Message
from .field_checker import (
    check_for_ros_2_mqtt_keys,
    check_for_service_keys,
    check_for_mqtt_2_ros_keys,
)
from .killable_timer import KillableTimer


class ROSBridge:
    def __init__(
        self,
        cfg: DictConfig,
        ros2mqtt_tasks: Dict[str, ROS2MQTTItem],
        mqtt2ros_tasks: Dict[str, MQTT2ROSItem],
    ):
        self.cfg = cfg
        self.ros2mqtt_tasks = ros2mqtt_tasks
        self.mqtt2ros_tasks = mqtt2ros_tasks
        self.ros_publishers: Dict[str, rospy.Publisher] = {}
        self.ros_service_clients: Dict[str, rospy.ServiceProxy] = {}
        self.ros_client_threads: Dict[str, KillableTimer] = {}


        self.create_ros_subscribers()
        self.create_ros_service_clients()

        # ROS client
        if "mqtt2ros" not in self.cfg or "loop_rate" not in self.cfg["mqtt2ros"]:
            self.loop_rate = 20
        else:
            self.loop_rate = self.cfg["mqtt2ros"]["loop_rate"]

        self.sleep_time = 1.0 / (self.loop_rate)  # default loop rate

        self.ros_publisher_available = "mqtt2ros" in self.cfg and (
            "topic2topic" in self.cfg["mqtt2ros"]
            and self.cfg["mqtt2ros"]["topic2topic"]
        )

        if self.ros_publisher_available:
            self.publisher_rate = {}
            self.create_ros_publishers()
            self.timer = rospy.timer.Timer(
                rospy.Duration(self.sleep_time), self.timer_callback
            )

    def create_ros_subscribers(self):
        if (
            "ros2mqtt" not in self.cfg
            or "topic2topic" not in self.cfg["ros2mqtt"]
            or not self.cfg["ros2mqtt"]["topic2topic"]
        ):
            rospy.logwarn(f"[ROS Bridge] No ROS2MQTT Topic 2 Topic found..")
            return
        # ros2mqtt (topic2topic)
        for ros_topic in self.cfg["ros2mqtt"]["topic2topic"]:
            ros2mqtt: Dict[str, str] = self.cfg["ros2mqtt"]["topic2topic"][ros_topic]
            check_for_ros_2_mqtt_keys("ROS", ros_topic, ros2mqtt)
            ros_type_name: str = ros2mqtt["ros_type"]
            mqtt_topic: str = ros2mqtt["mqtt_topic_name"]
            converter: str = ros2mqtt.get("converter", "")
            try:
                ros_type = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                rospy.logerr(f"The ros message type {ros_type_name} could not be found")
                raise

            self.ros2mqtt_tasks[mqtt_topic] = ROS2MQTTItem()

            rospy.Subscriber(
                name=ros_topic,
                data_class=ros_type,
                callback=partial(
                    self.ros_callback, converter=converter, ros_topic=ros_topic
                ),
                queue_size=10,
            )
            rospy.loginfo(f"[ROS Bridge] bridge <-  [ROS][topic]   {ros_topic}")
            rospy.loginfo(f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_topic}")

    def ros_callback(self, msg: MsgType, converter, ros_topic):
        rospy.logdebug(f"[ROS BRIDGE] Received [ROS][topic]: {ros_topic}")
        mqtt_topic: str = self.cfg["ros2mqtt"]["topic2topic"][ros_topic][
            "mqtt_topic_name"
        ]
        self.ros2mqtt_tasks[mqtt_topic].msg = msg
        self.ros2mqtt_tasks[mqtt_topic].converter = converter
        self.ros2mqtt_tasks[mqtt_topic].last_received_time = time.time()
        self.ros2mqtt_tasks[mqtt_topic].commmunication_type = CommunicationType.TOPIC2TOPIC

    def initialize_publish_rate(self, cfg: dict, publisher_key: str):
        if "rate" in cfg:
            given_rate = cfg["rate"]
            if given_rate > self.loop_rate:
                rospy.logwarn(
                    f"[ROS Bridge] rate given {cfg['rate']} is higher than loop rate, setting rate to loop rate {self.loop_rate}"
                )
                self.publisher_rate[publisher_key] = 1 / (
                    1 / self.loop_rate - self.sleep_time / 2
                )
            else:
                self.publisher_rate[publisher_key] = 1 / (
                    1 / given_rate - self.sleep_time / 2
                )
        else:
            self.publisher_rate[publisher_key] = 1 / (
                1 / self.loop_rate - self.sleep_time / 2
            )

    def create_ros_publishers(self):
        # mqtt2ros (topic2topic)
        if (
            "mqtt2ros" not in self.cfg
            or "topic2topic" not in self.cfg["ros2mqtt"]
            or not self.cfg["ros2mqtt"]["topic2topic"]
        ):
            rospy.logwarn(f"[ROS Bridge] No MQTT2ROS Topic 2 Topic found..")
            return

        for mqtt_topic in self.cfg["mqtt2ros"]["topic2topic"].keys():
            topic2topic: Dict[str, str] = self.cfg["mqtt2ros"]["topic2topic"][
                mqtt_topic
            ]
            check_for_mqtt_2_ros_keys("ROS", mqtt_topic, topic2topic)

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
            self.initialize_publish_rate(topic2topic, mqtt_topic)

            self.mqtt2ros_tasks[mqtt_topic] = MQTT2ROSItem()
            rospy.loginfo(f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_topic}")
            rospy.loginfo(f"[ROS Bridge]        ->  [ROS][topic]   {ros_topic}")

    def create_ros_service_clients(self):
        if (
            "mqtt2ros" not in self.cfg
            or "topic2service" not in self.cfg["mqtt2ros"]
            or not self.cfg["mqtt2ros"]["topic2service"]
        ):
            rospy.logwarn(f"[ROS Bridge] No ROS2MQTT Topic 2 Service found..")
            return
        # mqtt2ros (topic2service)
        for mqtt_request_topic in self.cfg["mqtt2ros"]["topic2service"]:
            topic2service: Dict[str, str] = self.cfg["mqtt2ros"]["topic2service"][
                mqtt_request_topic
            ]

            check_for_service_keys("ROS", mqtt_request_topic, topic2service)

            ros_service: str = topic2service["ros_service_name"]
            mqtt_response_topic: str = topic2service["mqtt_response_topic_name"]
            ros_type_name: str = topic2service["ros_type"]
            try:
                ros_type: MsgType = hydra.utils.get_class(ros_type_name)
            except ModuleNotFoundError:
                rospy.logerr(f"The ros message type {ros_type_name} could not be found")
                raise
            self.ros_service_clients[mqtt_request_topic] = rospy.ServiceProxy(
                name=ros_service, service_class=ros_type
            )
            self.mqtt2ros_tasks[mqtt_request_topic] = MQTT2ROSItem()
            self.ros2mqtt_tasks[mqtt_response_topic] = ROS2MQTTItem()
            rospy.loginfo(
                f"[ROS Bridge] bridge <-  [MQTT][topic]  {mqtt_request_topic}"
            )
            rospy.loginfo(f"[ROS Bridge]        <-> [ROS][service] {ros_service}")
            rospy.loginfo(
                f"[ROS Bridge]        ->  [MQTT][topic]  {mqtt_response_topic}"
            )

    def timer_callback(self, _):
        # Remove finished threads when iterating through the dict
        thread_to_remove = []
        for thread_name, thread in self.ros_client_threads.items():
            if not thread.is_alive():
                thread.join()
                rospy.logdebug(
                    f"[ROS Bridge] Service call for {thread_name} finished"
                )
                thread_to_remove.append(thread_name)
            elif thread.is_timed_out():
                    rospy.logwarn(
                        f"[ROS Bridge] Service call for {thread_name} timed out, cancelling the call"
                    )
                    thread.kill()
                    thread.join()
                    thread_to_remove.append(thread_name)
        for thread_name in thread_to_remove:
            del self.ros_client_threads[thread_name]


        for mqtt_receive_topic_name, mqtt2ros_item in self.mqtt2ros_tasks.items():
            current_time = time.time()
            if (
                mqtt2ros_item.last_received_time <= mqtt2ros_item.last_published_time
            ):  # no new message
                continue

            if mqtt2ros_item.command == "topic2topic":
                if (
                    current_time - mqtt2ros_item.last_published_time
                    < 1.0 / self.publisher_rate[mqtt_receive_topic_name]
                ):
                    continue

                ros_msg_typename: str = self.cfg["mqtt2ros"]["topic2topic"][
                    mqtt_receive_topic_name
                ]["ros_type"]
                self.ros_publishers[mqtt_receive_topic_name].publish(
                    convert_dictionary_to_ros_message(
                        hydra.utils.get_class(ros_msg_typename),
                        json.loads(mqtt2ros_item.payload),
                    )
                )
                mqtt2ros_item.last_published_time = current_time

            elif mqtt2ros_item.command == "topic2service":
                topic2service: Dict[str, Any] = self.cfg["mqtt2ros"]["topic2service"][
                    mqtt_receive_topic_name
                ]

                def service_call(
                    self: ROSBridge,
                    mqtt_receive_topic_name: str,
                    mqtt2ros_item: MQTT2ROSItem,
                    topic2service: Dict[str, Any],
                ):
                    ros_service: str = topic2service["ros_service_name"]
                    ros_srv_typename: str = topic2service["ros_type"]
                    timeout: Optional[float] = topic2service.get("timeout", None)
                    mqtt_response_topic_name: str = topic2service["mqtt_response_topic_name"]
                    try:
                        self.ros_service_clients[
                            mqtt_receive_topic_name
                        ].wait_for_service(timeout=timeout)
                    except rospy.exceptions.ROSException:
                        rospy.logerr(f"[ROS Bridge] Service {ros_service} timed out")
                        return

                    try:
                        request = convert_dictionary_to_ros_message(
                            hydra.utils.get_class(ros_srv_typename + "Request"),
                            json.loads(mqtt2ros_item.payload),
                        )
                    except AttributeError as e:
                        rospy.logerr(f"Erorr in converting ros message to json from {mqtt_receive_topic_name} to {ros_service} with {ros_srv_typename}")
                        rospy.logerr(e)
                        return

                    rospy.logdebug(
                        f"[ROS Bridge] Calling [ROS][service]: {ros_service}"
                    )

                    mqtt2ros_item.last_published_time = time.time()
                    try:
                        response = self.ros_service_clients[mqtt_receive_topic_name].call(
                            request
                        )
                    except rospy.ServiceException as e:
                        rospy.logerr(f"[ROS Bridge] Service call failed: {ros_service}, server is killed")
                        return


                    rospy.logdebug(f"[ROS Bridge] Successfully received Response [ROS][service]: {ros_service} with response:\n{response}")
                    self.ros2mqtt_tasks[mqtt_response_topic_name].msg = response
                    self.ros2mqtt_tasks[
                        mqtt_response_topic_name
                    ].converter = "primitive"
                    self.ros2mqtt_tasks[
                        mqtt_response_topic_name
                    ].last_received_time = time.time()
                    self.ros2mqtt_tasks[
                        mqtt_response_topic_name
                    ].commmunication_type = CommunicationType.TOPIC2SERVICE

                if mqtt_receive_topic_name in self.ros_client_threads:
                    if self.ros_client_threads[mqtt_receive_topic_name].is_alive():
                        rospy.logwarn(
                            f"[ROS Bridge] Previous service call for {mqtt_receive_topic_name} is still running, skipping this call"
                        )
                        mqtt2ros_item.last_published_time = current_time
                        continue
                self.ros_client_threads[mqtt_receive_topic_name] = KillableTimer(
                    function=service_call,
                    timeout=topic2service.get("timeout", None),
                    args=(self, mqtt_receive_topic_name, mqtt2ros_item, topic2service),
                )
                self.ros_client_threads[mqtt_receive_topic_name].start()
