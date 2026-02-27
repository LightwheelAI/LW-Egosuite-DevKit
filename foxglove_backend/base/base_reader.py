from abc import ABC, abstractmethod
import queue
import threading
# import multiprocessing as mp
import torch.multiprocessing as mp
import heapq
import math
import setproctitle
from collections import defaultdict
import traceback
import tqdm
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Generator, Tuple, Union, Optional

@dataclass(kw_only=True)
class BaseReader:
    raw_topic: str
    generateor_params: Optional[dict] = None

    def __post_init__(self, **kwargs):
        self.setup()
        self.match_processors()

    def setup(self):
        pass

    @abstractmethod
    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        yield None

    def __iter__(self):
        return self.generate_line()

    def match_processors(self):
        from foxglove_backend.visualizers import get_visualization_generators, MessageTypes
        extra_params = self.generateor_params or {}
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO,
            **extra_params
        )

    def get_topic2pb2(self):
        return {
            topic: pb_cls
            for generator in self.processors
            for topic, pb_cls in generator.outputs_topic2type.items()
        }

    def get_processors(self):
        return {self.raw_topic: self.processors}
