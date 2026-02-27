from abc import ABC, abstractmethod
import logging
import queue
import threading
# import multiprocessing as mp
import torch.multiprocessing as mp

logger = logging.getLogger(__name__)
import heapq
import math
import setproctitle
from collections import defaultdict
import traceback
import tqdm
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Generator, Tuple, Union, Optional

from .base_reader import BaseReader

from .base_writer import BaseWriter, writer_process


PROCESSORS = defaultdict(list)
LISTENED_TOPICS = set()


def process_worker(reses):
    buffer = []
    for src_topic, i, msg, listen_to_msg, ts in reses:
        try:
            for item in PROCESSORS[src_topic][i](msg, ts, *listen_to_msg):
                buffer.append(item)
        except Exception as e:
            logger.exception("Error processing %s", src_topic)
    return buffer

@dataclass(kw_only=True)
class BasePipeline:
    writer: BaseWriter
    start_ts: int = -math.inf
    end_ts: int = math.inf
    concurrency: Optional[int] = None
    chunk_size: int = 20

    def __post_init__(self):
        self.setup()

    def setup(self):
        self.readers = []
        self.output_topic2pb2 = {}
        self.set_readers()
        self.set_writer()

        self.read_line_then_worker_put_thread = threading.Thread(
            target=self._read_line_and_insert_to_worker_pool
        )
        self.pool_ref_queue = queue.Queue(maxsize=20)
        self.pool_ref_getter_thread = threading.Thread(
            target=self.pool_ref_getter
        )
        self.writer_queue = mp.Queue(maxsize=20)
        self.writer_process = mp.Process(
            target=writer_process,
            args=(self.writer, self.writer_queue)
        )

    def run(self):
        self.worker_pool = mp.Pool(processes=self.concurrency,
                                   initializer=lambda: setproctitle.setproctitle("lwviz worker"))
        self.pool_ref_getter_thread.start()
        self.writer_process.start()
        self._read_line_and_insert_to_worker_pool()
        self.pool_ref_getter_thread.join()
        self.writer_queue.close()
        self.writer_process.join()
        self.worker_pool.close()
        self.worker_pool.join()

    @abstractmethod
    def set_readers(self):
        raise NotImplementedError()

    def _add_reader(self, reader: BaseReader):
        self.readers.append(reader)
        self.output_topic2pb2.update(reader.get_topic2pb2())
        for src_topic, reader_processors in reader.get_processors().items():
            PROCESSORS[src_topic].extend(reader_processors)
            for processor in reader_processors:
                if processor.listen_to is not None:
                    LISTENED_TOPICS.update(processor.listen_to)

    @abstractmethod
    def set_writer(self):
        self.writer.set_topic2pb2(self.output_topic2pb2)

    def get_merge_queue(self):
        heapq_ = heapq.merge(
            *self.readers,
            key=lambda x: x
        )

        for src_topic, msg, ts in heapq_:
            if ts < self.start_ts:
                continue
            if ts > self.end_ts:
                break
            else:
                yield src_topic, msg, ts

    @staticmethod
    def chunk(gen, size):
        buffer = [None] * size
        i = None
        for count, res in enumerate(gen):
            i = count % size
            if i == 0 and count != 0:
                yield buffer
                buffer = [None] * size
            buffer[i] = res
        if i is not None:
            yield buffer[:i + 1]

    def filter(self, iter):
        initialize_msg = ["initialize", None, 0]
        try:
            first_item = next(iter)
            initialize_msg[2] = first_item[2]
            yield initialize_msg
            yield first_item
        except StopIteration:
            yield initialize_msg
        for item in iter:
            yield item

    def format(self, iter):
        for src_topic, msg, ts in iter:
            if src_topic in LISTENED_TOPICS:
                self.last_msg[src_topic] = msg
            for i, processor in enumerate(PROCESSORS[src_topic]):
                listen_to_msg = [
                    self.last_msg.get(listen_to_topic)
                    for listen_to_topic in processor.listen_to
                ] if processor.listen_to else ()
                yield src_topic, i, msg, listen_to_msg, ts

    def _read_line_and_insert_to_worker_pool(self):
        self.last_msg = {}
        try:
            for chunk in self.chunk(self.format(self.filter(self.get_merge_queue())), self.chunk_size):
                ref = self.worker_pool.apply_async(
                    process_worker, args=(chunk,)
                )
                self.pool_ref_queue.put(ref)
        finally:
            self.pool_ref_queue.put(None)

    def pool_ref_getter(self):
        try:
            while ref := self.pool_ref_queue.get():
                chunk = ref.get()
                self.writer_queue.put(chunk)
        finally:
            self.writer_queue.put(None)
