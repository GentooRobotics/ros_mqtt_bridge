# Deprecated: This file is deprecated
import rclpy
from rclpy.node import Node
from std_srvs.srv import Trigger
from robot_slam_msgs.srv import BTSaveMap, BTLoadMap
import time


class DummyStartMappingNode(Node):
    def __init__(self):
        super().__init__("dummy_start_mapping_node")
        self.start_srv = self.create_service(
            Trigger, "/bt/start_mapping", self.handle_start_mapping
        )
        self.stop_srv = self.create_service(
            Trigger, "/bt/stop_mapping", self.handle_stop_mapping
        )
        self.save_srv = self.create_service(
            BTSaveMap, "/bt/save_map", self.handle_save_map
        )
        self.load_srv = self.create_service(
            BTLoadMap, "/bt/load_map", self.handle_load_map
        )

    def handle_start_mapping(self, request, response):
        print("Start Mapping Service Received")
        response.success = True
        response.message = "Started Mapping"
        time.sleep(1)
        print("Start Mapping Service Responded")
        return response

    def handle_stop_mapping(self, request, response):
        print("Stop Mapping Service Received")
        response.success = True
        response.message = "Stopped Mapping"
        time.sleep(1)
        print("Stop Mapping Service Responded")
        return response

    def handle_save_map(self, request, response):
        print("Save Map Service Received")
        response.success = True
        response.message = "Map Saved"
        time.sleep(1)
        print("Save Map Service Responded")
        return response
    
    def handle_load_map(self, request, response):
        print("Load Map Service Received")
        response.success = True
        response.message = "Map Loaded"
        time.sleep(1)
        print("Load Map Service Responded")
        return response


def main(args=None):
    rclpy.init(args=args)
    node = DummyStartMappingNode()
    rclpy.spin(node)
    rclpy.shutdown()


if __name__ == "__main__":
    main()
