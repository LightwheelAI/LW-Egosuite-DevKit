from collections import defaultdict
from ..proto import get_pb2
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from enum import Enum, auto
from typing import List, Dict, Type, Any, Generator as GenType, Tuple
from functools import cached_property
from google.protobuf.message import Message as PbMessage


class MessageTypes(Enum):
    ROS = auto()
    PROTO = auto()



class Generator(ABC):
    listen_to = None

    @property
    def outputs(self) -> Dict[str, str]:
        return {}

    @cached_property
    def outputs_topic2type(self):
        return {
            topic: self.get_message_type(type_ident)
            for topic, type_ident in self.outputs.items()
        }

    def __init__(self, src_topic: str, message_type: MessageTypes, **kwargs):
        # wildcard matched works like "lightwheel/cam/*/rgb",
        # each matched topic will have a generator instance
        self.src_topic = src_topic
        self.message_type = message_type  # "proto"
        self.setup(**kwargs)
        self._time_cls = self.get_message_type("builtins/Time")
        self._duration_cls = self.get_message_type("builtins/Duration")

    def setup(self, **kwargs):
        pass

    @abstractmethod
    def generate(self, msg, timestamp, *_):
        raise NotImplementedError()

    def get_message_type(self, type_ident):
        res = None
        type_ident_is_pb_cls = isinstance(type_ident, type) and issubclass(type_ident, PbMessage)

        if self.message_type == MessageTypes.PROTO:
            if type_ident_is_pb_cls:
                res = type_ident
            else:
                res = get_pb2(type_ident)
        if not res:
            raise ValueError(self.message_type, type_ident, "not find")
        return res

    def __call__(self, msg: Any, timestamp: int, *listen_to_msgs) -> GenType[Tuple[str, str, int], None, None]:
        if self.message_type == MessageTypes.PROTO:
            for result in self.generate(msg, timestamp, *listen_to_msgs):
                # Support both (topic, msg) and (topic, msg, custom_timestamp)
                if len(result) == 3:
                    topic_name, msg, custom_timestamp = result
                    yield topic_name, msg.SerializeToString(), custom_timestamp
                else:
                    topic_name, msg = result
                    yield topic_name, msg.SerializeToString(), timestamp
        else:
            raise NotImplementedError()

    def setattr(self, msg, field, value):
        if self.message_type == MessageTypes.ROS:
            setattr(msg, field, value)
        elif self.message_type == MessageTypes.PROTO:
            getattr(msg, field).CopyFrom(value)

    @staticmethod
    def ns2sec_nsec(ns: int):
        fsec = ns / 1e9
        secs = int(fsec)
        nsecs = int((fsec - secs) * 1000000000)
        return secs, nsecs

    def get_time(self, ns):
        secs, nsecs = self.ns2sec_nsec(ns)
        if self.message_type == MessageTypes.ROS:
            return self._time_cls(secs, nsecs)
        elif self.message_type == MessageTypes.PROTO:
            return self._time_cls(sec=secs, nsec=nsecs)

    def set_builtins_time(self, ts_msg, ns: int):
        secs, nsecs = self.ns2sec_nsec(ns)
        ts_msg.seconds = secs
        ts_msg.nanos = nsecs

    def get_duration(self, ns):
        fsec = ns / 1e9
        secs = int(fsec)
        nsecs = int((fsec - secs) * 1000000000)
        if self.message_type == MessageTypes:
            return self._duration_cls(secs, nsecs)
        elif self.message_type == MessageTypes.PROTO:
            return self._duration_cls(sec=secs, nsec=nsecs)

    def set_header(self, msg, timestamp, frame_id="base_link"):
        self.setattr(msg.header, "stamp", self.get_time(timestamp))
        msg.header.frame_id = frame_id
