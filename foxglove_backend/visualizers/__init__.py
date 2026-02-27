from collections import defaultdict
from fnmatch import fnmatch
from typing import List, Dict, Type
from foxglove_backend.base.base_generator import Generator, MessageTypes


registry: Dict[str, List[Type["Generator"]]] = defaultdict(list)


def register(topic_pattern):
    def decorator(cls):
        if not hasattr(cls, "_registered_topics"):
            cls._registered_topics = []
        # Record the registered topic pattern
        cls._registered_topics.append(topic_pattern)

        registry[topic_pattern].append(cls)
        return cls
    return decorator


def get_visualization_generators(topic, message_type: MessageTypes, **kwargs) -> List["Generator"]:
    generators: List[Generator] = []
    for reg_topic_pattern, gen_clses in registry.items():
        if fnmatch(topic, reg_topic_pattern):
            for gen_cls in gen_clses:
                generators.append(gen_cls(
                    src_topic=topic,
                    message_type=message_type,
                    **kwargs
                ))
    return generators


from . import annotations  # noqa
from . import frame_tf  # noqa
from . import scene_update_generator  # noqa
from . import audio  # noqa
from . import low_quality  # noqa
from . import std_pcd_static  # noqa