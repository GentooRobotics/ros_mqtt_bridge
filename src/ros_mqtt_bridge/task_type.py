from dataclasses import dataclass, field
from enum import Enum, auto


class CommunicationType(Enum):
    TOPIC2TOPIC = auto()
    TOPIC2SERVICE = auto()


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
    commmunication_type: CommunicationType = field(
        default=CommunicationType.TOPIC2TOPIC
    )
