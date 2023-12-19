import cv2
import base64

from cv_bridge import CvBridge

bridge = CvBridge()


def image_serializer(msg):

    result = {}
    for data_field in msg.get_fields_and_field_types().keys():

        result[data_field] = None
        attr = getattr(msg, data_field)
        # print(f"{data_field}: {attr}")

        if "get_fields_and_field_types" in dir(attr):
            result[data_field] = image_serializer(attr)
        elif data_field == "data":
            image = bridge.imgmsg_to_cv2(msg)
            _, buffer = cv2.imencode(".png", image)
            image_data = base64.b64encode(buffer).decode("utf-8")
            result[data_field] = f"data:image/png;base64,{image_data}"
        else:
            result[data_field] = attr

    return result
