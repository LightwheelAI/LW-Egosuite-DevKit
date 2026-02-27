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



@dataclass
class BaseWriter:

    @abstractmethod
    def setup(self):
        pass

    def set_topic2pb2(self, topic2pb2):
        self.topic2pb2 = topic2pb2

    @abstractmethod
    def write_line(self, topic: str, msg, ts: int):
        pass

    def close(self):
        pass

    def __call__(self, *line):
        self.write_line(*line)

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def writer_process(writer: BaseWriter, writer_queue: mp.Queue):
    setproctitle.setproctitle(f"lwviz writer")

    def writer_queue_iter():
        while (msgs := writer_queue.get()) is not None:
            for msg in msgs:
                yield msg
    with writer:
        for item in tqdm.tqdm(writer_queue_iter()):
            writer(*item)
