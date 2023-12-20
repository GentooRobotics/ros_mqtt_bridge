import rospy


REQUIRED_MQTT_2_ROS_KEYS = ("ros_topic_name", "ros_type")
REQUIRED_ROS_2_MQTT_TOPIC_KEYS = ("mqtt_topic_name", "ros_type")
REQUIRED_SERVICE_KEYS = ("ros_service_name", "ros_type", "mqtt_response_topic_name")
    

def check_for_ros_2_mqtt_keys(bridge_type: str, ros_topic, ros2mqtt):
    if not all(key in ros2mqtt for key in REQUIRED_ROS_2_MQTT_TOPIC_KEYS):
        missing_keys = [key for key in REQUIRED_ROS_2_MQTT_TOPIC_KEYS if key not in ros2mqtt]
        rospy.logerr(f"[{bridge_type} Bridge] Skipping ROS2MQTT Topic 2 Topic {ros_topic} due to missing keys {missing_keys}")
        raise ValueError(f"Missing keys {missing_keys} in {ros_topic}")

def check_for_service_keys(bridge_type: str, mqtt_topic, mqtt2ros):
    if not all(key in mqtt2ros for key in REQUIRED_SERVICE_KEYS):
        missing_keys = [key for key in REQUIRED_SERVICE_KEYS if key not in mqtt2ros]
        rospy.logerr(f"[{bridge_type} Bridge] Skipping MQTT2ROS Topic 2 Service {mqtt_topic} due to missing keys {missing_keys}")
        raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")

def check_for_mqtt_2_ros_keys(bridge_type: str, mqtt_topic, mqtt2ros):
    if not all(key in mqtt2ros for key in REQUIRED_MQTT_2_ROS_KEYS):
        missing_keys = [key for key in REQUIRED_MQTT_2_ROS_KEYS if key not in mqtt2ros]
        rospy.logerr(f"[{bridge_type} Bridge] Skipping MQTT2ROS Topic 2 Topic {mqtt_topic} due to missing keys {missing_keys}")
        raise ValueError(f"Missing keys {missing_keys} in {mqtt_topic}")
