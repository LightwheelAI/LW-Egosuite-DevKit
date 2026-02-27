import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Tuple

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from foxglove_backend.base.base_reader import BaseReader
from foxglove_backend.visualizers import get_visualization_generators, MessageTypes
import numpy as np
import struct
from collections import defaultdict

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

def _voxel_key(x, y, z, voxel_size):
    """Voxel grid key (i, j, k)."""
    return (int(x / voxel_size), int(y / voxel_size), int(z / voxel_size))


class _SimplePCD:
    """Lightweight wrapper containing only pc_data, for pickling static-scene point clouds across processes (must be a module-level class)."""

    def __init__(self, pc_data):
        self.pc_data = pc_data


@dataclass(kw_only=True)
class StdPointCloudReader(BaseReader):
    """
    Read point cloud data from MCAP file and generate both per-frame point clouds
    and a static scene point cloud using voxel occupancy filtering.
    """

    file_path: Path
    static_voxel_size: float = 0.03
    static_occupancy_ratio: float = 0.005

    def __post_init__(self):
        super().__post_init__()
        self._static_scene_cache = None
        self._frame_pointclouds = []
        self._frame_timestamps = []

    def setup(self):
        self._reader = make_reader(self.file_path.open(
            "rb"), decoder_factories=[DecoderFactory()])

    def _pointcloud_msg_to_numpy(self, pointcloud_msg):
        """Convert foxglove.PointCloud message to numpy structured array."""
        if not pointcloud_msg.data:
            return None

        # Parse point cloud fields
        fields = {}
        for field in pointcloud_msg.fields:
            fields[field.name] = {"offset": field.offset, "type": field.type}

        # Extract point data
        point_stride = pointcloud_msg.point_stride
        data = pointcloud_msg.data
        num_points = len(data) // point_stride

        if num_points == 0:
            return None

        # Build dtype based on fields
        dtype = []
        for field_name, field_info in fields.items():
            if field_info["type"] == 7:  # FLOAT32
                dtype.append((field_name, np.float32))
            elif field_info["type"] == 1:  # UINT8
                dtype.append((field_name, np.uint8))

        if not dtype:
            return None

        # Create structured array
        points = np.zeros(num_points, dtype=dtype)

        for i in range(num_points):
            byte_offset = i * point_stride
            for field_name, field_info in fields.items():
                field_offset = byte_offset + field_info["offset"]
                if field_info["type"] == 7:  # FLOAT32
                    value = struct.unpack('<f', data[field_offset:field_offset+4])[0]
                    points[field_name][i] = value
                elif field_info["type"] == 1:  # UINT8
                    value = data[field_offset]
                    points[field_name][i] = value

        return points

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        """
        Read point cloud messages from MCAP and generate both per-frame and static scene point clouds.
        """

        # First pass: collect all point cloud data
        pointcloud_topic = "/pointcloud"
        for item in self._reader.iter_decoded_messages(topics=[pointcloud_topic]):
            if hasattr(item, "channel"):
                message = getattr(item, "message", None)
                msg = getattr(item, "decoded_message", None) or message
                timestamp = int(getattr(message, "log_time", 0))
            else:
                try:
                    _schema, _channel, message, decoded_message = item
                    msg = decoded_message or message
                    timestamp = int(getattr(message, "log_time", 0))
                except ValueError:
                    _topic, msg, timestamp = item

            # Convert point cloud message to numpy array
            pc_data = self._pointcloud_msg_to_numpy(msg)
            if pc_data is not None:
                self._frame_pointclouds.append(pc_data)
                self._frame_timestamps.append(timestamp)

        # Generate static scene point cloud
        static_scene = self._build_static_scene_accumulate_exclude_hands() #self._build_static_scene()
        if static_scene is not None:
            yield self.raw_topic, static_scene, self._frame_timestamps[0]

    def _build_static_scene_accumulate_exclude_hands(self):
        """Filter out points near the hand for each frame, accumulate all remaining points, and then perform voxel downsampling to obtain a complete environment point cloud."""
        if not self._frame_pointclouds:
            return None

        pose_data = self._read_pose_data()
        if not pose_data:
            logger.warning(
                "[StdPointCloudReader] No pose data found; cannot use accumulate_exclude_hands, skipping static scene")
            return None

        voxel_size = self.static_voxel_size
        radius = 0.2  #
        radius_sq = radius * radius

        # voxel_key -> list of (x,y,z,r,g,b,a)
        voxel_to_points = defaultdict(list)

        for frame_idx, pc_data in enumerate(self._frame_pointclouds):
            if not hasattr(pc_data.dtype, "names") or not pc_data.dtype.names or "x" not in pc_data.dtype.names:
                continue

            frame_pose = self._get_frame_pose(pose_data, frame_idx)
            if not frame_pose:
                continue

            hand_positions = self._extract_hand_positions(frame_pose)

            x_out = np.asarray(pc_data["x"], dtype=np.float64)
            y_out = np.asarray(pc_data["y"], dtype=np.float64)
            z_out = np.asarray(pc_data["z"], dtype=np.float64)
            r = np.asarray(pc_data["red"], dtype=np.float64)
            g = np.asarray(pc_data["green"], dtype=np.float64)
            b = np.asarray(pc_data["blue"], dtype=np.float64)
            a = np.asarray(pc_data["alpha"], dtype=np.float64)


            keep_mask = np.ones(len(x_out), dtype=bool)

            for hand_pos in hand_positions:
                dx = x_out - hand_pos[0]
                dy = y_out - hand_pos[1]
                dz = z_out - hand_pos[2]
                dist_sq = dx*dx + dy*dy + dz*dz

                keep_mask &= (dist_sq >= radius_sq)

            x_valid = x_out[keep_mask]
            y_valid = y_out[keep_mask]
            z_valid = z_out[keep_mask]
            r_valid = r[keep_mask]
            g_valid = g[keep_mask]
            b_valid = b[keep_mask]
            a_valid = a[keep_mask]

            for xv, yv, zv, rv, gv, bv, av in zip(x_valid, y_valid, z_valid, r_valid, g_valid, b_valid, a_valid):
                key = _voxel_key(xv, yv, zv, voxel_size)
                voxel_to_points[key].append((xv, yv, zv, rv, gv, bv, av))

        if not voxel_to_points:
            logger.warning(
                "[StdPointCloudReader] No voxels for static scene (after hand filtering), skipping static point cloud")
            return None

        static_points = []
        for key, points_list in voxel_to_points.items():
            arr = np.array(points_list, dtype=[("x", np.float64), ("y", np.float64), ("z", np.float64),
                                            ("red", np.float64), ("green", np.float64), ("blue", np.float64), ("alpha", np.float64)])
            mean_xyz = np.array(
                [arr["x"].mean(), arr["y"].mean(), arr["z"].mean()])
            mean_rgba = np.array(
                [arr["red"].mean(), arr["green"].mean(), arr["blue"].mean(), arr["alpha"].mean()])
            static_points.append((*mean_xyz, *mean_rgba))

        static_points = np.array(static_points)
        n = len(static_points)
        dtype = [("x", np.float32), ("y", np.float32), ("z", np.float32),
                ("red", np.uint8), ("green", np.uint8), ("blue", np.uint8), ("alpha", np.uint8)]
        static_pc = np.empty(n, dtype=dtype)
        static_pc["x"] = static_points[:, 0].astype(np.float32)
        static_pc["y"] = static_points[:, 1].astype(np.float32)
        static_pc["z"] = static_points[:, 2].astype(np.float32)
        static_pc["red"] = np.clip(static_points[:, 3], 0, 255).astype(np.uint8)
        static_pc["green"] = np.clip(static_points[:, 4], 0, 255).astype(np.uint8)
        static_pc["blue"] = np.clip(static_points[:, 5], 0, 255).astype(np.uint8)
        static_pc["alpha"] = np.clip(static_points[:, 6], 0, 255).astype(np.uint8)

        self._static_scene_cache = {"pcd_data": _SimplePCD(static_pc), "static_scene": True}
        return self._static_scene_cache

    def _read_pose_data(self):
        """Read pose data from MCAP file."""
        def _tf_to_dict(tf_obj: Any) -> Dict[str, Any]:
            quat = getattr(tf_obj, "quat", None)
            return {
                "x": float(getattr(tf_obj, "x", 0.0)),
                "y": float(getattr(tf_obj, "y", 0.0)),
                "z": float(getattr(tf_obj, "z", 0.0)),
                "quat": {
                    "w": float(getattr(quat, "w", 1.0)),
                    "x": float(getattr(quat, "x", 0.0)),
                    "y": float(getattr(quat, "y", 0.0)),
                    "z": float(getattr(quat, "z", 0.0)),
                },
            }
        pose_data = {
        }

        left_hand_pose_topic = "/pose/left_hand"

        for item in self._reader.iter_decoded_messages(topics=[left_hand_pose_topic]):
            message = getattr(item, "message", None)
            msg = getattr(item, "decoded_message", None) or message
            timestamp = int(getattr(message, "log_time", 0))

            transforms = list(getattr(msg, "transforms", []))
            tf_dicts = [_tf_to_dict(tf_obj) for tf_obj in transforms]

            pose_data[timestamp] = []
            for pose in tf_dicts:
                pose_data[timestamp].append({
                    'x': pose['x'],
                    'y': pose['y'],
                    'z': pose['z']
                })

        right_hand_pose_topic = "/pose/right_hand"
        for item in self._reader.iter_decoded_messages(topics=[right_hand_pose_topic]):
            message = getattr(item, "message", None)
            msg = getattr(item, "decoded_message", None) or message
            timestamp = int(getattr(message, "log_time", 0))

            transforms = list(getattr(msg, "transforms", []))
            tf_dicts = [_tf_to_dict(tf_obj) for tf_obj in transforms]

            for pose in tf_dicts:
                pose_data[timestamp].append({
                    'x': pose['x'],
                    'y': pose['y'],
                    'z': pose['z']
                })
        return pose_data

    def _get_frame_pose(self, pose_data, frame_idx):
        """Get pose data for specific frame index."""
        if not self._frame_timestamps or frame_idx >= len(self._frame_timestamps):
            return None

        frame_timestamp = self._frame_timestamps[frame_idx]

        closest_timestamp = None
        min_diff = float('inf')

        for timestamp in pose_data.keys():
            diff = abs(timestamp - frame_timestamp)
            if diff < min_diff:
                min_diff = diff
                closest_timestamp = timestamp

        if min_diff > 100000000:  # 100ms
            return None

        return pose_data.get(closest_timestamp)

    def _extract_hand_positions(self, frame_pose):
        """Extract hand joint positions from pose data."""
        hand_positions = []
        for pos in frame_pose:
            hand_positions.append(np.array([pos['x'], pos['y'], pos['z']]))
        return hand_positions