^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package ros_mqtt_bridge
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

0.2.0 (2024-01-07)
------------------
* Fix bug for multiple ROS Service response
* Fix bug if config value is empty for topic2topic and topic2service
* Add timeout for service call to terminate thread if service call takes too long

0.1.0 (2023-12-20)
------------------
* Initial implementation of `ros_mqtt_bridge` package
* Handles publishing and subscribing to ROS topics and MQTT topics
* Handles calling ROS services from MQTT topics and publish the response to MQTT
* Limit the number of messages to be published to ROS/MQTT based on specified rate
