import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Tuple

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from lw_egosuite_backend.base.base_reader import BaseReader
from lw_egosuite_backend.visualizers import get_visualization_generators, MessageTypes
import numpy as np
import struct

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
        self._reader = make_reader(self.file_path.open(
            "rb"), decoder_factories=[DecoderFactory()])
        # Logical topic used inside the pipeline / visualizers.
        self.raw_topic = "subtask-annotation"

    def match_processors(self):
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        """
        Read /annotation/semantic_segments and expand into per-frame annotations.

        Supports two MCAP shapes produced by annotation_segments_json_to_proto:
        - tier1/tier2: first message is tier1 (caption only, no time range); rest are tier2
          with label, start_time, end_time (seconds). Caption from tier1 plays for whole episode.
        - annotations: segments with description, skill, start_frame, end_frame.
        """
        # Build frame timeline first (needed for both formats and for out_start_frame/end_frame).
        raw_reader = make_reader(self.file_path.open("rb"))
        frame_timestamps = _read_pose_body_frame_timestamps(raw_reader)
        total_frames = len(frame_timestamps)
        if not total_frames:
            return
        episode_start_ns = frame_timestamps[0]

        episode_caption = ""
        segments = []
        for msg, _ in _iter_decoded(self._reader, "/annotation/semantic_segments"):
            # New proto: each MCAP message is one segment.
            # task_description is top-level; segment is a singular sub-message.
            task = str(getattr(msg, "task_description", "") or "")
            if task and not episode_caption:
                episode_caption = task

            seg = getattr(msg, "segment", None)
            if seg is None:
                continue

            subtask = str(getattr(seg, "subtask_description", "") or "")
            # skill is now a repeated field — join into a single string
            skill_list = list(getattr(seg, "skill", []) or [])
            skill = ", ".join(str(s) for s in skill_list)
            start_time = getattr(seg, "start_time", None)
            end_time = getattr(seg, "end_time", None)

            if start_time is not None and end_time is not None:
                segments.append({
                    "by_time": True,
                    "description": subtask,
                    "skill": skill,
                    "start_time_sec": float(start_time),
                    "end_time_sec": float(end_time),
                })

        def _segment_start_key(s):
            return s["start_time_sec"] if s["by_time"] else s["start_frame"]
        segments = sorted(segments, key=_segment_start_key)

        stage_index = 0
        for frame_idx in range(total_frames):
            timestamp_ns = frame_timestamps[frame_idx]
            relative_sec = (timestamp_ns - episode_start_ns) / 1_000_000_000.0

            current_segment = None
            while stage_index < len(segments):
                stage = segments[stage_index]
                if stage["by_time"]:
                    if relative_sec < stage["start_time_sec"]:
                        break
                    if stage["start_time_sec"] <= relative_sec <= stage["end_time_sec"]:
                        current_segment = stage
                        break
                else:
                    if frame_idx < stage["start_frame"]:
                        break
                    if stage["start_frame"] <= frame_idx <= stage["end_frame"]:
                        current_segment = stage
                        break
                stage_index += 1

            if current_segment and current_segment["by_time"]:
                out_start_frame = 0
                out_end_frame = total_frames - 1
                for fi, ts_ns in enumerate(frame_timestamps):
                    t_sec = (ts_ns - episode_start_ns) / 1_000_000_000.0
                    if t_sec >= current_segment["start_time_sec"]:
                        out_start_frame = fi
                        break
                for fi in range(total_frames - 1, -1, -1):
                    t_sec = (frame_timestamps[fi] -
                             episode_start_ns) / 1_000_000_000.0
                    if t_sec <= current_segment["end_time_sec"]:
                        out_end_frame = fi
                        break
            else:
                out_start_frame = int(
                    current_segment["start_frame"]) if current_segment else 0
                out_end_frame = int(current_segment["end_frame"]) if current_segment else (
                    total_frames - 1)

            sec = int(timestamp_ns // 1_000_000_000)
            nanos = int(timestamp_ns % 1_000_000_000)
            data = {
                "frame_number": int(frame_idx),
                "timestamp_seconds": sec,
                "timestamp_nanos": nanos,
                "has_annotation": bool(current_segment is not None),
                "description": {
                    "caption": episode_caption,
                    "description": current_segment["description"] if current_segment else "",
                    "skill": current_segment.get("skill", "") if current_segment else "",
                },
                "start_frame": out_start_frame,
                "end_frame": out_end_frame,
            }
            yield self.raw_topic, data, int(timestamp_ns)


# -----------------------------------------------------------------------------
# Shared helpers (MCAP iteration)
# -----------------------------------------------------------------------------

def _read_pose_body_frame_timestamps(reader) -> list:
    """Read frame timestamps from /pose/head on the given reader."""
    out = []
    for _schema, channel, message in reader.iter_messages(topics=["/pose/head"]):
        if channel.topic == "/pose/head":
            out.append(int(getattr(message, "log_time", 0)))
    if not out:
        raise ValueError(
            "No /pose/head messages found in input MCAP. "
            "Cannot build frame timeline for annotation visualization."
        )
    return out


def _iter_decoded(reader, topic: str) -> Generator[Tuple[Any, int], None, None]:
    """Iterate decoded messages for topic; yield (decoded_msg, log_time_ns)."""
    for item in reader.iter_decoded_messages(topics=[topic]):
        if hasattr(item, "channel"):
            message = getattr(item, "message", None)
            msg = getattr(item, "decoded_message", None) or message
            ts = int(getattr(message, "log_time", 0))
        else:
            try:
                _s, _c, message, decoded_message = item
                msg = decoded_message or message
                ts = int(getattr(message, "log_time", 0))
            except ValueError:
                _topic, msg, ts = item
        yield msg, ts
