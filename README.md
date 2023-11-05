## MQTT Bridge

This is a ROS package that allows two ways bridging between ROS & MQTT protocols. MQTT Bridge supports 4 types of bridging.
- `ROS topic -> MQTT topic`
- `ROS service -> MQTT topic [TO DO]`
- `MQTT topic -> ROS topic`
- `MQTT topic -> ROS service`

### Config
#### ROS topic-MQTT topic Bridge
By default, The `primitive_serializer` converter is used which recursively iterate through the ROS message and serialize it into JSON format. Any custom converter can be implemented in `./ros_serializers/custom_serializers.py`
```yaml
ros2mqtt:
  topic2topic:
    /chatter: # ROS topic
      to: chatter # MQTT topic
      ros_type: std_msgs.msg.String # ROS Message Type
```

#### MQTT topic-ROS service Bridge 
```yaml
mqtt2ros:
  topic2topic:
    number: # MQTT topic 
      to: /number # ROS topic
      ros_type: std_msgs.msg.Int32 # ROS Message type
```

#### MQTT topic-ROS service Bridge 
```yaml
mqtt2ros:
  topic2service:
    backend/start_mapping: # MQTT topic 
      to: /bt/start_mapping # ROS service
      ros_type: std_srvs.srv.Trigger # ROS service type
      response: bt/start_mapping_response # MQTT response topic
```



### Usage
1. Move the `mqtt_bridge` package into a workspace
    ```sh
    mkdir -p colcon_ws/src
    mv mqtt_bridge colcon_ws/src
    cd colcon_ws
    ```
2. Build Docker image
    ```sh
    docker pull perceptronproject/mqtt_bridge:arm64-v0.1.0
    ```
3. Run Docker Container
    ```sh
    ./src/mqtt_bridge/scripts/create_container.bash
    ```
4. Run the package
    ```sh
    python3 ./src/mqtt_bridge/mqtt_bridge.py
    ```
### Testing
1. Run the Package
    ```sh
    # Run the Container
    ./src/mqtt_bridge/scripts/create_container.bash

    # Run the Package
    ./src/mqtt_bridge/tests/scripts/entrypoint.sh
    ```
2. In a separate terminal, Run PyTest
    ```sh
    # Run the Container
    ./src/mqtt_bridge/scripts/create_container.bash

    # Run PyTest
    pytest
    ```