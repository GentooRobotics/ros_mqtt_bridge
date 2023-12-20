from dataclasses import dataclass, field


@dataclass
class MQTT2ROSItem:
    last_published_time: float = field(default=0.0)
    last_received_time: float = field(default=0.0)
    payload: bytes = field(default=bytes())
    command: str = field(default="")


@dataclass
class ROS2MQTTItem:
    last_published_time: float = field(default=0.0)
    last_received_time: float = field(default=0.0)
    msg: object = field(default=None)
    converter: str = field(default="")
