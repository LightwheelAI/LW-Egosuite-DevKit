import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

import tyro
from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from foxglove_backend.base.base_pipeline import BasePipeline
from foxglove_backend.mcap_writer import MCAPWriter
from foxglove_backend.converters.std_mcap import (
    StdAnnotationPerFrameReader,
    StdLowQualityReader,
)
from foxglove_backend.converters.pose.std_mcap import (
    StdPoseDataReader,
    StdPoseSceneReader,
    StdPoseTFReader,
    StdHeadPoseTrajectoryReader,
)


logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class StdPipeline(BasePipeline):
    """
    Standard MCAP → visualization MCAP pipeline.

    Usage:
        python -m foxglove_backend.std_pipeline \\
            --path in.mcap \\
            --writer.path out.mcap
    """

    path: Path
    """Input MCAP path."""

    writer: MCAPWriter
    """MCAP writer for the output visualization file."""

    metadata_mcap: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Auto-set output path based on input filename if using default path
        self.writer.set_output_path_from_input(self.path)
        super().__post_init__()

    def _load_session_metadata(self):
        """
        Read /session/metadata from input MCAP and convert to file-level metadata
        schema used by existing pipelines.
        """
        self.metadata_mcap = {}
        reader = make_reader(self.path.open(
            "rb"), decoder_factories=[DecoderFactory()])
        session_msg = None
        session_ts = None
        for item in reader.iter_decoded_messages(topics=["/session/metadata"]):
            if hasattr(item, "decoded_message"):
                session_msg = item.decoded_message
                message = getattr(item, "message", None)
                session_ts = int(getattr(message, "log_time", 0))
            else:
                try:
                    # type: ignore[misc]
                    _schema, _channel, message, decoded_message = item
                    session_msg = decoded_message
                    session_ts = int(getattr(message, "log_time", 0))
                except ValueError:
                    # Not expected, but keep robust.
                    # type: ignore[misc]
                    _topic, session_msg, session_ts = item
            if session_msg is not None:
                break

        if session_msg is None:
            logger.warning(
                "No /session/metadata found in input MCAP; skip file-level metadata.")
            self.metadata_mcap = {}
            return

        task_info = getattr(session_msg, "task_info", None)
        operator = getattr(session_msg, "operator", None)
        devices = list(getattr(session_msg, "devices", []))

        start_ns = None
        end_ns = None
        if devices:
            starts = [int(getattr(d, "unix_start_time_ms", 0))
                      for d in devices if getattr(d, "unix_start_time_ms", 0)]
            ends = [int(getattr(d, "unix_end_time_ms", 0))
                    for d in devices if getattr(d, "unix_end_time_ms", 0)]
            if starts:
                start_ns = min(starts) * 1_000_000
            if ends:
                end_ns = max(ends) * 1_000_000
        # Fallback to record timestamp.
        if start_ns is None and session_ts is not None:
            start_ns = int(session_ts)
        if end_ns is None and session_ts is not None:
            end_ns = int(session_ts)

        self.metadata_mcap = {
            "task_id": getattr(task_info, "task_name", "") if task_info is not None else "",
            "environment_id": getattr(task_info, "environment_id", "") if task_info is not None else "",
            "session_uuid": getattr(task_info, "episode_uuid", "") if task_info is not None else "",
            "operator_id": getattr(operator, "operator_id", "") if operator is not None else "",
            "instruction": getattr(task_info, "task_description", "") if task_info is not None else "",
            "environment_description": getattr(task_info, "environment_description", "") if task_info is not None else "",
            "task_description": getattr(task_info, "task_description", "") if task_info is not None else "",
            "start_time_unix_ns": int(start_ns or 0),
            "end_time_unix_ns": int(end_ns or 0),
        }

    def set_readers(self):
        # Clear and then add our MCAP-based readers.
        self.output_topic2pb2 = {}
        self._load_session_metadata()

        # 1) Annotation: /annotation/per_frame -> subtask-annotation -> lightwheel.SubtaskAnnotation
        self._add_reader(
            StdAnnotationPerFrameReader(
                file_path=self.path,
                raw_topic="subtask-annotation",
            )
        )
        self._add_reader(
            StdLowQualityReader(
                file_path=self.path,
                raw_topic="low-quality-annotation",
            )
        )

        # 2) Pose topics -> shared reader -> tf / scene / head trajectory.
        pose_reader = StdPoseDataReader(file_path=self.path)
        self._add_reader(
            StdPoseTFReader(
                pose_data_reader=pose_reader,
                raw_topic="tf-tree",
            )
        )
        self._add_reader(
            StdPoseSceneReader(
                pose_data_reader=pose_reader,
                raw_topic="scene-update",
            )
        )
        self._add_reader(
            StdHeadPoseTrajectoryReader(
                pose_data_reader=pose_reader,
                raw_topic="scene-update/head_pose_trajectory",
                points_number_to_show=int(30 * 1.5),
            )
        )


    def set_writer(self):
        # BasePipeline expects writer.topic2pb2 to be populated.
        self.writer.set_topic2pb2(self.output_topic2pb2)
        if self.metadata_mcap:
            self.writer.add_metadata("metadata", self.metadata_mcap)


def main():
    """
    CLI entry so that:

        python -m foxglove_backend.std_pipeline --path in.mcap --writer.path out.mcap

    works and behaves consistently with the other pipelines.
    """
    from foxglove_backend.logging_config import setup_logging

    setup_logging()
    tyro.extras.set_accent_color("magenta")
    pipeline = tyro.cli(StdPipeline)
    pipeline.run()


if __name__ == "__main__":
    main()
