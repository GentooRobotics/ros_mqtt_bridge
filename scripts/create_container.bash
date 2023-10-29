#!/bin/bash

docker run -it --rm \
    --network host \
    --volume="./:/colcon_ws" \
    -w="/colcon_ws" \
    mqtt_bridge \
    bash
