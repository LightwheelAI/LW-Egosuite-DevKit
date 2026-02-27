from dataclasses import dataclass, field
from mcap.writer import Writer, CompressionType
from foxglove_backend.proto import get_proto_descriptor_bin
from .base import BaseWriter
import logging
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class MCAPWriter(BaseWriter):
    path: Path = Path("./output/output_vis.mcap")
    """output path, should *.mcap"""
    compression: CompressionType = CompressionType.ZSTD
    """Compression algorithm to use"""
    metadata_dict: Dict[str, Dict[str, str]] = field(default_factory=dict)
    """File-level metadata, formatted as {name: {key: value}}"""

    def set_output_path_from_input(self, input_path: Path):
        """
        Set output path based on input path.
        Generates output filename as {input_filename}_vis.mcap
        """
        if self.path == Path("./output/output_vis.mcap"):
            # Only auto-generate if using default path
            # Use stem to get filename without extension

            input_filename = input_path.stem
            output_filename = f"{input_filename}_vis.mcap"
            output_dir = input_path.parent / "output"
            output_dir.mkdir(exist_ok=True)
            self.path = output_dir / output_filename

    def setup(self):
        self.f = open(self.path, "wb")
        self.mcap_writer = Writer(self.f, compression=self.compression)
        self.mcap_writer.start()
        if not hasattr(self, "topic2pb2"):
            raise RuntimeError(
                "topic2pb2 not set, call set_topic2pb2 before with")
        self.topic2channel_id: Dict[str, int] = {}
        for topic, pb_cls in self.topic2pb2.items():
            self.register_channel(topic, pb_cls)

    def set_topic2pb2(self, topic2pb2: Dict[str, Any]):
        self.topic2pb2 = topic2pb2

    def register_channel(self, topic: str, pb_cls: Any):
        # Support 2 schema sources:
        # 1) local pb2 class (legacy behavior)
        # 2) raw schema spec dict from input mcap passthrough
        if isinstance(pb_cls, dict) and "schema_data" in pb_cls:
            schema_name = pb_cls.get("schema_name", topic)
            schema_encoding = pb_cls.get("schema_encoding", "protobuf")
            schema_data = pb_cls["schema_data"]
            message_encoding = pb_cls.get("message_encoding", "protobuf")
        else:
            schema_name = pb_cls.DESCRIPTOR.full_name
            schema_encoding = "protobuf"
            schema_data = get_proto_descriptor_bin(pb_cls)
            message_encoding = "protobuf"

        schema_id = self.mcap_writer.register_schema(
            name=schema_name,
            encoding=schema_encoding,
            data=schema_data
        )
        channel_id = self.mcap_writer.register_channel(
            schema_id=schema_id,
            topic=topic,
            message_encoding=message_encoding,
        )
        self.topic2channel_id[topic] = channel_id
        return channel_id

    def write_line(self, topic: str, msg, ts: int):
        # Serialize the protobuf message (if not already serialized)
        if hasattr(msg, 'SerializeToString'):
            # This is a protobuf message object that needs to be serialized.
            serialized_msg = msg.SerializeToString()
        else:
            # Already serialized bytes
            serialized_msg = msg

        self.mcap_writer.add_message(
            channel_id=self.topic2channel_id[topic],
            log_time=ts,
            data=serialized_msg,
            publish_time=ts
        )

    def add_metadata(self, name: str, metadata: Dict[str, str]):
        """Add file-level metadata to the MCAP file (will be written upon close).
        :param name: Name of the metadata
        :param metadata: Dictionary of key-value pairs; all values will be converted to strings
        """
        self.metadata_dict[name] = metadata

    def close(self):
        # Add all metadata before closing
        if self.metadata_dict:
            for name, metadata in self.metadata_dict.items():
                assert type(metadata) == dict
                str_metadata = {k: str(v) for k, v in metadata.items()}
                self.mcap_writer.add_metadata(name=name, data=str_metadata)
        self.mcap_writer.finish()
        self.f.close()
        logger.info("MCAP file written to %s", self.path)
        super().close()
