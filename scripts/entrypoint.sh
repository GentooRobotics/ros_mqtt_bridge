#!/bin/bash
source install/setup.bash
python3 ./src/mqtt_bridge/mqtt_bridge.py & python3 ./src/mqtt_bridge/tests/dummy_mapping.py & ros2 bag play ./src/mqtt_bridge/tests/assets/map_with_laser_bag -l