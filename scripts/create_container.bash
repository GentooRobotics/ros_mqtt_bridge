#!/bin/bash
if [ "$1" == "robot" ]; then
    docker run -it --rm \
    --network host \
    --volume="./:/colcon_ws" \
    --volume="/proj/turtlebot4_discovery:/turtlebot4_discovery" \
    --volume="/dev/shm:/dev/shm" \
    -w="/colcon_ws" \
    -e RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
    -e FASTRTPS_DEFAULT_PROFILES_FILE=/turtlebot4_discovery/fastdds_discovery_super_client.xml \
    -e ROS_DISCOVERY_SERVER=10.121.48.149:11811 \
    -e ROS_DOMAIN_ID=0 \
    mqtt_bridge \
    bash
else
    docker run -it --rm \
    --network host \
    --volume="./:/colcon_ws" \
    --volume="/dev/shm:/dev/shm" \
    -w="/colcon_ws" \
    mqtt_bridge \
    bash
fi
