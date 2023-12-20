import cv2
import base64

from cv_bridge import CvBridge
from message_converter import convert_ros_message_to_dictionary
from sensor_msgs.msg import Image

bridge = CvBridge()


def image_serializer(msg: Image):
    result = {}
    image = bridge.imgmsg_to_cv2(msg)
    _, buffer = cv2.imencode(".png", image)
    image_data = base64.b64encode(buffer).decode("utf-8")
    result["data"] = f"data:image/png;base64,{image_data}"
    result["header"] = convert_ros_message_to_dictionary(msg.header)
    result["height"] = msg.height
    result["width"] = msg.width
    result["encoding"] = msg.encoding
    result["is_bigendian"] = msg.is_bigendian
    result["step"] = msg.step
    return result
