#!/usr/bin/env python3

# import rclpy
# from rclpy.node import Node
# from std_srvs.srv import Trigger

# class StartMappingServer(Node):
#     def __init__(self):
#         super().__init__('start_mapping_server')
#         self.srv = self.create_service(Trigger, '/bt/start_mapping', self.start_mapping_callback)
#         self.get_logger().info('Start Mapping Service Server is up and running.')

#     def start_mapping_callback(self, request, response):
#         self.get_logger().info('Received a start_mapping request')
#         # You can add your mapping start logic here
#         response.success = True
#         response.message = 'Mapping started successfully'
#         return response

# def main(args=None):
#     rclpy.init(args=args)
#     start_mapping_server = StartMappingServer()
#     rclpy.spin(start_mapping_server)
#     rclpy.shutdown()

# if __name__ == '__main__':
#     main()


# Create a simple ROS 1 Server SetBool Server

import time
import rospy
from std_srvs.srv import SetBool, SetBoolRequest, SetBoolResponse

def handle_set_bool(req):
    print("Returning [%s]"%(req.data))
    response =  SetBoolResponse()
    time.sleep(10)
    response.success = True
    response.message = "Set bool successfully"
    return response

def set_bool_server():
    rospy.init_node('set_bool_server')
    s = rospy.Service('set_bool', SetBool, handle_set_bool)
    print("Ready to set bool.")
    rospy.spin()

if __name__ == "__main__":
    set_bool_server()