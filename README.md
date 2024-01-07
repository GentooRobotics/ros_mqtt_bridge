## ROS MQTT Bridge
[![Version badge](https://img.shields.io/badge/Version-0.2.0-green.svg)](https://shields.io/)


This is a ROS package that allows two ways bridging between ROS & MQTT protocols. MQTT Bridge supports 4 types of bridging.
- `ROS topic -> MQTT topic`
- `MQTT topic -> ROS topic`
- `MQTT topic -> ROS service`

### Config
#### ROS topic-MQTT topic Bridge
By default, The `primitive_serializer` converter is used which recursively iterate through the ROS message and serialize it into JSON format. Any custom converter can be implemented in `./ros_serializers/custom_serializers.py`. These yaml file is defined under [bridge.yaml](param/bridge.yaml)

```yaml
ros2mqtt:
  loop_rate: 20
  topic2topic:
    /chatter: # ROS topic
      mqtt_topic_name: chatter # MQTT topic
      ros_type: std_msgs.msg.String # ROS Message Type
      rate: 1 # Optional maximum publishing rate

    /map_with_laser:
      mqtt_topic_name: map_with_laser
      ros_type: sensor_msgs.msg.Image
      converter: image_serializer # Custom serialization
```

#### MQTT topic-ROS topic Bridge 
```yaml
mqtt2ros:
  loop_rate: 20
  topic2topic:
    number: # MQTT topic 
      ros_topic_name: /number # ROS topic
      ros_type: std_msgs.msg.Int32 # ROS Message type
      rate: 1 # Optional rate parameters 

```

#### MQTT topic-ROS service Bridge 
```yaml
mqtt2ros:
  topic2service:
    backend/start_mapping: # MQTT topic 
      ros_service_name: /bt/start_mapping # ROS service
      ros_type: std_srvs.srv.Trigger # ROS service type
      mqtt_response_topic_name: bt/start_mapping_response # MQTT response topic
```


### Prerequisites
The package must be running with Python 3.8 with ROS Noetic.

To install the necessary dependencies, run the following command:
```sh
pip3 install -r requirements.txt
```
Similarly, a [Dockerfile](Dockerfile) is provided to setup the necessary requirements.


### Usage
1. Move the `ros_mqtt_bridge` package into a workspace
    ```sh
    mkdir -p catkin_ws/src
    mv ros_mqtt_bridge catkin_ws/src
    cd catkin_ws
    ```
    
2. Build the package
    ```sh
    catkin build ros_mqtt_bridge
    ```
3. Run the package
    ```sh
    roslaunch ros_mqtt_bridge ros_mqtt_bridge.launch
    ```