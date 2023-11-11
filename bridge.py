import rclpy
import hydra
import signal 
from omegaconf import DictConfig
from queue import Queue
from threading import Thread
from typing import Tuple
from rclpy.publisher import MsgType

from mqtt_bridge import MQTTBridge 
from ros_bridge import ROSBridge

@hydra.main(version_base=None, config_path="./configs", config_name="bridge")
def main(cfg: DictConfig) -> None:

    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
 
    rclpy.init()
    
    ros2mqtt_tasks: Queue[Tuple[str, str, MsgType, str]] = Queue(maxsize=cfg["ros2mqtt"]["queue_size"])
    mqtt2ros_tasks: Queue[Tuple[str, str, str]] = Queue(maxsize=cfg["ros2mqtt"]["queue_size"])

    # ROS Bridge Thread
    ros_bridge = ROSBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)
    ros_bridge_thread = Thread(target=rclpy.spin, args=(ros_bridge,))

    # MQTT Bridge Thread
    mqtt_bridge = MQTTBridge(cfg, ros2mqtt_tasks, mqtt2ros_tasks)
    mqtt_bridge_thread = Thread(target=mqtt_bridge.run)

    ros_bridge_thread.start()
    mqtt_bridge_thread.start()

    mqtt_bridge.shutdown_flag.set()

    # Wait for the threads to close...
    ros_bridge_thread.join()
    mqtt_bridge_thread.join() 

if __name__ == "__main__":
    main()