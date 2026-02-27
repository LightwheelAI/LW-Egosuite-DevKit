from codecs import ascii_encode
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Tuple

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from foxglove_backend.base.base_reader import BaseReader
from foxglove_backend.visualizers import get_visualization_generators, MessageTypes

logger = logging.getLogger(__name__)

@dataclass(kw_only=True)
class StdAnnotationPerFrameReader(BaseReader):
    """
    Read /annotation/per_frame from an input MCAP and convert it to the same
    intermediate dict format that AnnotationsGenerator expects, emitting on the
    logical topic 'subtask-annotation'.
    """

    file_path: Path

    def setup(self):
        self._reader = make_reader(self.file_path.open("rb"), decoder_factories=[DecoderFactory()])
        # Logical topic used inside the pipeline / visualizers.
        self.raw_topic = "subtask-annotation"

    def match_processors(self):
        # Reuse existing annotations visualizer.
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        """
        Read /annotation/segments (annotation.segments.AnnotationSegment) and
        expand segments into per-frame annotations, compatible with
        AnnotationsGenerator input.
        """
        segments = []
        segment_topic = "/annotation/segments"
        for item in self._reader.iter_decoded_messages(topics=[segment_topic]):
            if hasattr(item, "channel"):
                message = getattr(item, "message", None)
                msg = getattr(item, "decoded_message", None) or message
            else:
                try:
                    _schema, _channel, message, decoded_message = item  # type: ignore[misc]
                    msg = decoded_message or message
                except ValueError:
                    _topic, msg, _src_ts = item  # type: ignore[misc]

            segment = getattr(msg, "segment", None)
            if segment is None:
                continue
            segments.append(
                {
                    "description": str(getattr(segment, "description", "")),
                    "skill": str(getattr(segment, "skill", "")),
                    "start_frame": int(getattr(segment, "start_frame", 0)),
                    "end_frame": int(getattr(segment, "end_frame", 0)),
                }
            )

        if not segments:
            return

        # Sort by start frame (same idea as annotation.py).
        segments = sorted(segments, key=lambda x: x["start_frame"])

        # Use /pose/body timestamps as per-frame timeline.
        frame_timestamps = []
        raw_reader = make_reader(self.file_path.open("rb"))
        for _schema, channel, message in raw_reader.iter_messages(topics=["/pose/body"]):
            if channel.topic == "/pose/body":
                frame_timestamps.append(int(getattr(message, "log_time", 0)))

        total_frames = len(frame_timestamps)

        stage_index = 0
        for frame_idx in range(total_frames):
            current_segment = None
            while stage_index < len(segments):
                stage = segments[stage_index]
                if frame_idx < stage["start_frame"]:
                    break
                if stage["start_frame"] <= frame_idx <= stage["end_frame"]:
                    current_segment = stage
                    break
                stage_index += 1


            timestamp_ns = frame_timestamps[frame_idx]
            sec = int(timestamp_ns // 1_000_000_000)
            nanos = int(timestamp_ns % 1_000_000_000)

            data = {
                "frame_number": int(frame_idx),
                "timestamp_seconds": sec,
                "timestamp_nanos": nanos,
                "has_annotation": bool(current_segment is not None),
                "description": {
                    "description":current_segment["description"] if current_segment else "",
                    "skill": current_segment["skill"] if current_segment else "",
                },
                # "skill": current_segment["skill"] if current_segment else "",
                "skill": "",
                "start_frame": int(current_segment["start_frame"]) if current_segment else 0,
                "end_frame": int(current_segment["end_frame"]) if current_segment else (total_frames - 1),
            }
            yield self.raw_topic, data, int(timestamp_ns)






@dataclass(kw_only=True)
class StdLowQualityReader(BaseReader):
    """
    Read /annotation/low_quality summary from standard MCAP, map frame_ids to
    per-frame timestamps, and emit low-quality annotations.
    """

    file_path: Path

    def setup(self):
        self._reader = make_reader(self.file_path.open("rb"), decoder_factories=[DecoderFactory()])
        self.raw_topic = "low-quality-annotation"

    def match_processors(self):
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        # 1) Build frame index -> timestamp map from /pose/body.
        frame_timestamps = []
        raw_reader = make_reader(self.file_path.open("rb"))
        for _schema, channel, message in raw_reader.iter_messages(topics=["/pose/body"]):
            if channel.topic == "/pose/body":
                frame_timestamps.append(int(getattr(message, "log_time", 0)))

        if not frame_timestamps:
            return

        # 2) Read low-quality summary and aggregate multiple problem types per frame.
        frame_to_types = {}
        for item in self._reader.iter_decoded_messages(topics=["/annotation/low_quality"]):
            if hasattr(item, "channel"):
                msg = getattr(item, "decoded_message", None) or getattr(item, "message", None)
            else:
                try:
                    _schema, _channel, _message, decoded_message = item  # type: ignore[misc]
                    msg = decoded_message
                except ValueError:
                    _topic, msg, _ts = item  # type: ignore[misc]

            problem_types = list(getattr(msg, "problem_types", []) or [])
            for pt in problem_types:
                name = str(getattr(pt, "name", "unknown"))
                frame_ids = list(getattr(pt, "frame_ids", []) or [])
                for fid in frame_ids:
                    idx = int(fid)
                    if idx < 0 or idx >= len(frame_timestamps):
                        continue
                    frame_to_types.setdefault(idx, []).append(name)

        # 3) Emit one message per frame so empty frames clear previous text.
        for frame_idx in range(len(frame_timestamps)):
            ts_ns = int(frame_timestamps[frame_idx])
            sec = int(ts_ns // 1_000_000_000)
            nanos = int(ts_ns % 1_000_000_000)
            yield self.raw_topic, {
                "frame_number": int(frame_idx),
                "timestamp_seconds": sec,
                "timestamp_nanos": nanos,
                "problem_types": frame_to_types.get(frame_idx, []),
            }, ts_ns
