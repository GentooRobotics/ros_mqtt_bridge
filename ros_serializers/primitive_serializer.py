from rclpy.publisher import MsgType

def primitive_serializer(msg: MsgType):
    return {
        field: (
            primitive_serializer(getattr(msg, field))
            if "get_fields_and_field_types" in dir(getattr(msg, field))
            else getattr(msg, field)
        )
        for field in msg.get_fields_and_field_types().keys()
    }
