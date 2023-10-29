import sys

sys.path.append("../../")

from std_msgs.msg import String, Int32, Float32, Bool, ByteMultiArray
import ros_serializers


def test_serializer_on_string():
    msg = String()
    msg.data = "Hello World"
    serialized = ros_serializers.primitive_serializer(msg)

    assert serialized == {"data": msg.data}


def test_serializer_on_int32():
    msg = Int32()
    msg.data = 10
    serialized = ros_serializers.primitive_serializer(msg)

    assert serialized == {"data": msg.data}


def test_serializer_on_float32():
    msg = Float32()
    msg.data = 10.0
    serialized = ros_serializers.primitive_serializer(msg)

    assert serialized == {"data": msg.data}


def test_serializer_on_bool():
    msg = Bool()
    msg.data = True
    serialized = ros_serializers.primitive_serializer(msg)

    assert serialized == {"data": msg.data}


def test_serializer_multi_level():
    msg = ByteMultiArray()
    serialized = ros_serializers.primitive_serializer(msg)

    assert serialized == {"layout": {"dim": [], "data_offset": 0}, "data": []}
