import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import tqdm
from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from lw_egosuite_backend.base.base_reader import BaseReader
from lw_egosuite_backend.converters.pose.std_mcap import _build_pose_frames
from lw_egosuite_backend.visualizers import get_visualization_generators, MessageTypes
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
        self._reader = make_reader(self.file_path.open(
            "rb"), decoder_factories=[DecoderFactory()])
        # Logical topic used inside the pipeline / visualizers.
        self.raw_topic = "subtask-annotation"

    def match_processors(self):
        # Reuse existing annotations visualizer.
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        """
        Read /annotation/segments and expand into per-frame annotations.

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
        for msg, _ in _iter_decoded(self._reader, "/annotation/segments"):
            seg = getattr(msg, "segment", None)
            if seg is None:
                continue

            description = str(getattr(seg, "description", "") or "")
            skill = str(getattr(seg, "skill", "") or "")
            start_frame = getattr(seg, "start_frame", None)
            end_frame = getattr(seg, "end_frame", None)

            caption = str(getattr(seg, "caption", "") or "")
            label = str(getattr(seg, "label", "") or "")
            start_time = getattr(seg, "start_time", None)
            end_time = getattr(seg, "end_time", None)

            # tier1-only: caption set, no meaningful time range (0,0), no label → episode caption
            st_sec = float(start_time) if start_time is not None else 0.0
            et_sec = float(end_time) if end_time is not None else 0.0
            has_frames = start_frame is not None and end_frame is not None
            if caption and not label and st_sec == 0 and et_sec == 0:
                episode_caption = caption
                continue
            # tier2: label + start_time/end_time (seconds)
            if (start_time is not None and end_time is not None) and (label or st_sec != 0 or et_sec != 0):
                segments.append({
                    "by_time": True,
                    "description": label,
                    "start_time_sec": st_sec,
                    "end_time_sec": et_sec,
                })
                continue
            # annotations: description/skill + start_frame/end_frame
            if has_frames:
                segments.append({
                    "by_time": False,
                    "description": description,
                    "skill": skill,
                    "start_frame": int(start_frame),
                    "end_frame": int(end_frame),
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


@dataclass(kw_only=True)
class StdLowQualityReader(BaseReader):
    """
    Read /annotation/low_quality summary from standard MCAP, map frame_ids to
    per-frame timestamps, and emit low-quality annotations.
    """

    file_path: Path

    def setup(self):
        self._reader = make_reader(self.file_path.open(
            "rb"), decoder_factories=[DecoderFactory()])
        self.raw_topic = "low-quality-annotation"

    def match_processors(self):
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        raw_reader = make_reader(self.file_path.open("rb"))
        frame_timestamps = _read_pose_body_frame_timestamps(raw_reader)
        if not frame_timestamps:
            return

        frame_to_types = {}
        for msg, _ in _iter_decoded(self._reader, "/annotation/low_quality"):
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


def _voxel_key(x: float, y: float, z: float, voxel_size: float):
    """Voxel grid key (i, j, k)."""
    return (int(x / voxel_size), int(y / voxel_size), int(z / voxel_size))


def _radius_center_from_mcap_frame(frame: Dict[str, Any]) -> Tuple[Optional[np.ndarray], str]:
    """
    Pose center for cropping: mean of body joints in world frame (standard MCAP body TFs are world);
    if body is empty, mean of left and right hand joints.
    """
    body = frame["body"]
    if len(body) > 0:
        coords = np.array(
            [[float(j["x"]), float(j["y"]), float(j["z"])] for j in body],
            dtype=np.float64,
        )
        return coords.mean(axis=0), f"body mean ({len(body)} joints, world frame)"
    left_hand = frame["left_hand"]
    right_hand = frame["right_hand"]
    parts: List[List[float]] = []
    for j in left_hand:
        parts.append([float(j["x"]), float(j["y"]), float(j["z"])])
    for j in right_hand:
        parts.append([float(j["x"]), float(j["y"]), float(j["z"])])
    if not parts:
        return None, "no body and no hand points"
    stacked = np.array(parts, dtype=np.float64)
    n_l, n_r = len(left_hand), len(right_hand)
    return stacked.mean(axis=0), f"hands mean (left {n_l} + right {n_r} = {n_l + n_r} joints)"


def _filter_np_pc_pose_radius(
    pc_data: np.ndarray, center: np.ndarray, radius_m: float
) -> np.ndarray:
    """Keep points within radius_m of center (vectorized)."""
    r2 = radius_m ** 2
    x = np.asarray(pc_data["x"], dtype=np.float64)
    y = np.asarray(pc_data["y"], dtype=np.float64)
    z = np.asarray(pc_data["z"], dtype=np.float64)
    dx = x - center[0]
    dy = y - center[1]
    dz = z - center[2]
    keep = (dx * dx + dy * dy + dz * dz) <= r2
    return pc_data[keep]


def _filter_np_pc_statistical_outliers(
    pc_data: np.ndarray,
    enabled: bool,
    nb_neighbors: int,
    std_ratio: float,
) -> np.ndarray:
    """SOR: drop points whose mean k-NN distance exceeds global mean + std_ratio * std."""
    if not enabled:
        return pc_data
    n = len(pc_data)
    k = int(nb_neighbors)
    if n < 2 or k < 1:
        return pc_data
    from scipy.spatial import cKDTree

    xyz = np.column_stack(
        (
            np.asarray(pc_data["x"], dtype=np.float64),
            np.asarray(pc_data["y"], dtype=np.float64),
            np.asarray(pc_data["z"], dtype=np.float64),
        )
    )
    k_query = min(k + 1, n)
    if k_query < 2:
        return pc_data
    tree = cKDTree(xyz)
    try:
        dists, _ = tree.query(xyz, k=k_query, workers=-1)
    except TypeError:
        dists, _ = tree.query(xyz, k=k_query)
    mean_nn_dist = np.mean(dists[:, 1:], axis=1)
    mu = float(np.mean(mean_nn_dist))
    sigma = float(np.std(mean_nn_dist))
    if sigma < 1e-12:
        return pc_data
    threshold = mu + std_ratio * sigma
    keep = mean_nn_dist <= threshold
    return pc_data[keep]


def _arm_segment_endpoints_from_pose(frame_dict: Dict[str, Any]) -> List[Tuple[np.ndarray, np.ndarray]]:
    """Return [(shoulder, middle_tip), ...] for left/right arms when body joints are available."""
    body = frame_dict["body"]
    if len(body) <= 17:
        return []
    left_hand = frame_dict["left_hand"]
    right_hand = frame_dict["right_hand"]
    if len(left_hand) <= 12 or len(right_hand) <= 12:
        return []

    left_shoulder = np.array(
        [float(body[16]["x"]), float(body[16]["y"]), float(body[16]["z"])], dtype=np.float64
    )
    right_shoulder = np.array(
        [float(body[17]["x"]), float(body[17]["y"]), float(body[17]["z"])], dtype=np.float64
    )
    left_middle_tip = np.array(
        [float(left_hand[12]["x"]), float(left_hand[12]["y"]), float(left_hand[12]["z"])], dtype=np.float64
    )
    right_middle_tip = np.array(
        [float(right_hand[12]["x"]), float(right_hand[12]["y"]), float(right_hand[12]["z"])], dtype=np.float64
    )
    return [
        (left_shoulder, left_middle_tip),
        (right_shoulder, right_middle_tip),
    ]


def _compute_arm_box_keep_mask(
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
    arm_segments: List[Tuple[np.ndarray, np.ndarray]],
    *,
    arm_half_width_m: float,
    arm_half_height_m: float,
    arm_axis_padding_m: float,
) -> np.ndarray:
    """
    Keep mask for points outside all arm-oriented boxes.
    Each box is centered on shoulder->middle_tip segment with configurable thickness.
    """
    points = np.column_stack((x, y, z))
    keep_mask = np.ones(points.shape[0], dtype=bool)
    eps = 1e-9

    for shoulder, middle_tip in arm_segments:
        arm_vec = middle_tip - shoulder
        arm_len = float(np.linalg.norm(arm_vec))
        if arm_len <= eps:
            continue
        arm_axis = arm_vec / arm_len
        ref_axis = np.array([0.0, 0.0, 1.0], dtype=np.float64)
        if abs(float(np.dot(arm_axis, ref_axis))) > 0.95:
            ref_axis = np.array([0.0, 1.0, 0.0], dtype=np.float64)

        side_axis_1 = np.cross(arm_axis, ref_axis)
        side_axis_1_norm = float(np.linalg.norm(side_axis_1))
        if side_axis_1_norm <= eps:
            continue
        side_axis_1 = side_axis_1 / side_axis_1_norm
        side_axis_2 = np.cross(arm_axis, side_axis_1)

        center = (shoulder + middle_tip) * 0.5
        rel = points - center
        axis_coord = rel @ arm_axis
        side_coord_1 = rel @ side_axis_1
        side_coord_2 = rel @ side_axis_2

        axis_half_len = arm_len * 0.5 + arm_axis_padding_m
        inside_box = (
            (np.abs(axis_coord) <= axis_half_len)
            & (np.abs(side_coord_1) <= arm_half_width_m)
            & (np.abs(side_coord_2) <= arm_half_height_m)
        )
        keep_mask &= ~inside_box

    return keep_mask


class _StdPCFilterLogFlags:
    """One-time English log lines for pose radius and SOR (per reader instance)."""

    pose_radius_miss_logged: bool = False
    pose_radius_hit_logged: bool = False
    sor_logged: bool = False


def _apply_std_pointcloud_pose_radius_and_sor(
    pc_data: np.ndarray,
    frame_dict: Dict[str, Any],
    frame_idx: int,
    log_tag: str,
    *,
    log_prefix: str,
    pcd_pose_radius_filter_m: float,
    pcd_sor_enabled: bool,
    pcd_sor_nb_neighbors: int,
    pcd_sor_std_ratio: float,
    flags: _StdPCFilterLogFlags,
) -> np.ndarray:
    """Pose-centered radius crop (optional) then statistical outlier removal; mutates log flags."""
    r_m = pcd_pose_radius_filter_m
    if r_m > 0:
        center, center_desc = _radius_center_from_mcap_frame(frame_dict)
        if center is None:
            if not flags.pose_radius_miss_logged:
                logger.warning(
                    "%s Pose radius filter (%s, frame_idx=%d): radius %.4f m, %s — skipping crop",
                    log_prefix,
                    log_tag,
                    frame_idx,
                    r_m,
                    center_desc,
                )
                flags.pose_radius_miss_logged = True
        else:
            if not flags.pose_radius_hit_logged:
                # logger.info(
                #     "%s Pose radius filter (%s, frame_idx=%d): radius %.4f m, center=%s, xyz=(%.4f, %.4f, %.4f)",
                #     log_prefix,
                #     log_tag,
                #     frame_idx,
                #     r_m,
                #     center_desc,
                #     float(center[0]),
                #     float(center[1]),
                #     float(center[2]),
                # )
                flags.pose_radius_hit_logged = True
            pc_data = _filter_np_pc_pose_radius(pc_data, center, r_m)

    if pcd_sor_enabled:
        n_before = len(pc_data)
        k = int(pcd_sor_nb_neighbors)
        pc_data = _filter_np_pc_statistical_outliers(
            pc_data,
            True,
            k,
            float(pcd_sor_std_ratio),
        )
        if not flags.sor_logged and n_before >= 2 and k >= 1:
            # logger.info(
            #     "%s Sparse SOR (%s, frame_idx=%d): k=%d, std_ratio=%.2f, points %d -> %d",
            #     log_prefix,
            #     log_tag,
            #     frame_idx,
            #     k,
            #     float(pcd_sor_std_ratio),
            #     n_before,
            #     len(pc_data),
            # )
            flags.sor_logged = True

    return pc_data


def _closest_pose_frame_dict(
    pose_by_ts: Dict[int, Dict[str, Any]], query_ts: int, max_diff_ns: int
) -> Optional[Dict[str, Any]]:
    """Return pose frame dict at query_ts or nearest key within max_diff_ns."""
    if query_ts in pose_by_ts:
        return pose_by_ts[query_ts]
    closest_ts = None
    min_diff = float("inf")
    for ts in pose_by_ts.keys():
        d = abs(ts - query_ts)
        if d < min_diff:
            min_diff = d
            closest_ts = ts
    if closest_ts is None or min_diff > max_diff_ns:
        return None
    return pose_by_ts[closest_ts]


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
    # Crop to a sphere around pose (body mean, else hands mean); <=0 disables.
    pcd_pose_radius_filter_m: float = 3.0
    # Statistical outlier removal: drop points with high mean k-NN distance.
    pcd_sor_enabled: bool = True
    pcd_sor_nb_neighbors: int = 20
    pcd_sor_std_ratio: float = 2.0
    pose_align_max_diff_ns: int = 100_000_000

    def __post_init__(self):
        super().__post_init__()
        self._static_scene_cache = None
        self._frame_pointclouds = []
        self._frame_timestamps = []
        self._pose_by_ts: Dict[int, Dict[str, Any]] = {}
        self._pc_filter_flags = _StdPCFilterLogFlags()

    def setup(self):
        self._reader = make_reader(self.file_path.open(
            "rb"), decoder_factories=[DecoderFactory()])

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        """
        Read point cloud messages from MCAP and generate both per-frame and static scene point clouds.
        """

        self._pose_by_ts = _build_pose_frames(self.file_path)

        for msg, timestamp in tqdm.tqdm(
            _iter_decoded(self._reader, "/pointcloud"),
            desc="pointcloud",
            miniters=10,
            mininterval=0.5,
        ):
            pc_data = _pointcloud_msg_to_numpy(msg)
            if pc_data is not None:
                self._frame_pointclouds.append(pc_data)
                self._frame_timestamps.append(timestamp)

        # Generate static scene point cloud
        # self._build_static_scene()
        static_scene = self._build_static_scene_accumulate_exclude_hands()
        if static_scene is not None:
            yield self.raw_topic, static_scene, self._frame_timestamps[0]

    def _build_static_scene_accumulate_exclude_hands(self):
        """Filter out points near the hand for each frame, accumulate all remaining points, and then perform voxel downsampling to obtain a complete environment point cloud."""
        if not self._frame_pointclouds:
            return None

        if not self._pose_by_ts:
            logger.warning(
                "[StdPointCloudReader] No pose data in MCAP; cannot use accumulate_exclude_hands, skipping static scene"
            )
            return None

        voxel_size = self.static_voxel_size
        radius = 0.2
        radius_sq = radius * radius
        arm_half_width_m = 0.14
        arm_half_height_m = 0.12
        arm_axis_padding_m = 0.06

        # voxel_key -> list of (x,y,z,r,g,b,a)
        voxel_to_points = defaultdict(list)

        for frame_idx, pc_data in enumerate(self._frame_pointclouds):
            if not hasattr(pc_data.dtype, "names") or not pc_data.dtype.names or "x" not in pc_data.dtype.names:
                continue

            frame_dict = self._get_frame_pose_dict(frame_idx)
            if not frame_dict:
                continue

            pc_data = self._apply_pose_radius_and_sor(
                pc_data, frame_dict, frame_idx, log_tag="static_scene"
            )

            x_out = np.asarray(pc_data["x"], dtype=np.float64)
            y_out = np.asarray(pc_data["y"], dtype=np.float64)
            z_out = np.asarray(pc_data["z"], dtype=np.float64)
            r = np.asarray(pc_data["red"], dtype=np.float64)
            g = np.asarray(pc_data["green"], dtype=np.float64)
            b = np.asarray(pc_data["blue"], dtype=np.float64)
            a = np.asarray(pc_data["alpha"], dtype=np.float64)

            arm_segments = _arm_segment_endpoints_from_pose(frame_dict)
            if arm_segments:
                keep_mask = _compute_arm_box_keep_mask(
                    x_out,
                    y_out,
                    z_out,
                    arm_segments,
                    arm_half_width_m=arm_half_width_m,
                    arm_half_height_m=arm_half_height_m,
                    arm_axis_padding_m=arm_axis_padding_m,
                )
            else:
                hand_positions = self._extract_hand_positions(frame_dict)
                keep_mask = np.ones(len(x_out), dtype=bool)
                for hand_pos in hand_positions:
                    dx = x_out - hand_pos[0]
                    dy = y_out - hand_pos[1]
                    dz = z_out - hand_pos[2]
                    dist_sq = dx * dx + dy * dy + dz * dz
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
        static_pc["red"] = np.clip(
            static_points[:, 3], 0, 255).astype(np.uint8)
        static_pc["green"] = np.clip(
            static_points[:, 4], 0, 255).astype(np.uint8)
        static_pc["blue"] = np.clip(
            static_points[:, 5], 0, 255).astype(np.uint8)
        static_pc["alpha"] = np.clip(
            static_points[:, 6], 0, 255).astype(np.uint8)

        self._static_scene_cache = {
            "pcd_data": _SimplePCD(static_pc), "static_scene": True}
        return self._static_scene_cache

    def _get_frame_pose_dict(self, frame_idx: int) -> Optional[Dict[str, Any]]:
        """Return the MCAP pose frame dict closest to the point cloud timestamp."""
        if not self._frame_timestamps or frame_idx >= len(self._frame_timestamps):
            return None
        return _closest_pose_frame_dict(
            self._pose_by_ts,
            self._frame_timestamps[frame_idx],
            self.pose_align_max_diff_ns,
        )

    def _apply_pose_radius_and_sor(
        self,
        pc_data: np.ndarray,
        frame_dict: Dict[str, Any],
        frame_idx: int,
        *,
        log_tag: str,
    ) -> np.ndarray:
        """Apply pose-centered radius crop (optional) then statistical outlier removal."""
        return _apply_std_pointcloud_pose_radius_and_sor(
            pc_data,
            frame_dict,
            frame_idx,
            log_tag,
            log_prefix="[StdPointCloudReader]",
            pcd_pose_radius_filter_m=self.pcd_pose_radius_filter_m,
            pcd_sor_enabled=self.pcd_sor_enabled,
            pcd_sor_nb_neighbors=self.pcd_sor_nb_neighbors,
            pcd_sor_std_ratio=self.pcd_sor_std_ratio,
            flags=self._pc_filter_flags,
        )

    def _extract_hand_positions(self, frame_dict: Dict[str, Any]) -> List[np.ndarray]:
        """Hand joint positions in world frame from a merged pose frame dict."""
        hand_positions: List[np.ndarray] = []
        for j in frame_dict["left_hand"]:
            hand_positions.append(
                np.array([float(j["x"]), float(j["y"]), float(j["z"])])
            )
        for j in frame_dict["right_hand"]:
            hand_positions.append(
                np.array([float(j["x"]), float(j["y"]), float(j["z"])])
            )
        return hand_positions


# -----------------------------------------------------------------------------
# Shared helpers (MCAP iteration, point cloud conversion)
# -----------------------------------------------------------------------------

def _pointcloud_msg_to_numpy(pointcloud_msg) -> Optional[np.ndarray]:
    """Convert foxglove.PointCloud message to numpy structured array."""
    if not pointcloud_msg.data:
        return None
    fields = {}
    for field in pointcloud_msg.fields:
        fields[field.name] = {"offset": field.offset, "type": field.type}
    point_stride = pointcloud_msg.point_stride
    data = pointcloud_msg.data
    num_points = len(data) // point_stride
    if num_points == 0:
        return None
    dtype = []
    for field_name, field_info in fields.items():
        if field_info["type"] == 7:  # FLOAT32
            dtype.append((field_name, np.float32))
        elif field_info["type"] == 1:  # UINT8
            dtype.append((field_name, np.uint8))
    if not dtype:
        return None
    points = np.zeros(num_points, dtype=dtype)
    for i in range(num_points):
        byte_offset = i * point_stride
        for field_name, field_info in fields.items():
            field_offset = byte_offset + field_info["offset"]
            if field_info["type"] == 7:
                points[field_name][i] = struct.unpack("<f", data[field_offset : field_offset + 4])[0]
            elif field_info["type"] == 1:
                points[field_name][i] = data[field_offset]
    return points


def _read_pose_body_frame_timestamps(reader) -> list:
    """Read frame timestamps from /pose/body on the given reader."""
    out = []
    for _schema, channel, message in reader.iter_messages(topics=["/pose/body"]):
        if channel.topic == "/pose/body":
            out.append(int(getattr(message, "log_time", 0)))
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


@dataclass(kw_only=True)
class StdPerFramePointCloudReader(BaseReader):
    """
    Emit one point cloud message per frame (from /pose/body). Frames with point cloud
    data get that data; frames without get an empty PointCloud.
    """

    file_path: Path
    raw_topic: str = "pointcloud/2d_projection"
    max_time_diff_ns: int = 100_000_000  # 100ms
    # Crop to a sphere around pose (body mean, else hands mean); <=0 disables.
    pcd_pose_radius_filter_m: float = 3.0
    pcd_sor_enabled: bool = True
    pcd_sor_nb_neighbors: int = 20
    pcd_sor_std_ratio: float = 2.0
    pose_align_max_diff_ns: int = 100_000_000

    def __post_init__(self):
        super().__post_init__()
        self._pc_filter_flags = _StdPCFilterLogFlags()

    def setup(self):
        self._reader = make_reader(
            self.file_path.open("rb"), decoder_factories=[DecoderFactory()]
        )

    def match_processors(self):
        from lw_egosuite_backend.visualizers import (
            get_visualization_generators,
            MessageTypes,
        )
        self.processors = get_visualization_generators(
            self.raw_topic, MessageTypes.PROTO
        )

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        frame_timestamps = _read_pose_body_frame_timestamps(self._reader)
        if not frame_timestamps:
            return

        pose_by_ts = _build_pose_frames(self.file_path)
        empty_pose_frame: Dict[str, Any] = {
            "body": [],
            "left_hand": [],
            "right_hand": [],
        }

        pc_list: list = []
        for msg, timestamp in _iter_decoded(self._reader, "/pointcloud"):
            pc_data = _pointcloud_msg_to_numpy(msg)
            if pc_data is not None:
                pc_list.append((timestamp, pc_data))

        max_diff = self.max_time_diff_ns
        # Time alignment: assign each point cloud to the temporally closest frame
        # (within 100ms); at most one pc per frame; unmatched frames get empty.
        frame_to_pc: Dict[int, Tuple[int, Any]] = {}  # frame_ts -> (pc_ts, pc_data)
        for pc_ts, pc_data in pc_list:
            best_frame_ts = None
            best_diff = max_diff + 1
            for frame_ts in frame_timestamps:
                d = abs(pc_ts - frame_ts)
                if d < best_diff:
                    best_diff = d
                    best_frame_ts = frame_ts
            if best_frame_ts is None:
                continue
            # If multiple pcs claim the same frame, keep the one with smaller time diff
            existing = frame_to_pc.get(best_frame_ts)
            if existing is None or best_diff < abs(existing[0] - best_frame_ts):
                frame_to_pc[best_frame_ts] = (pc_ts, pc_data)

        for frame_idx, frame_ts in enumerate(frame_timestamps):
            assigned = frame_to_pc.get(frame_ts)
            if assigned is not None:
                _pc_ts, pc_data = assigned
                frame_dict = _closest_pose_frame_dict(
                    pose_by_ts, frame_ts, self.pose_align_max_diff_ns
                )
                if frame_dict is None:
                    frame_dict = empty_pose_frame
                pc_data = _apply_std_pointcloud_pose_radius_and_sor(
                    pc_data,
                    frame_dict,
                    frame_idx,
                    "per_frame",
                    log_prefix="[StdPerFramePointCloudReader]",
                    pcd_pose_radius_filter_m=self.pcd_pose_radius_filter_m,
                    pcd_sor_enabled=self.pcd_sor_enabled,
                    pcd_sor_nb_neighbors=self.pcd_sor_nb_neighbors,
                    pcd_sor_std_ratio=self.pcd_sor_std_ratio,
                    flags=self._pc_filter_flags,
                )
                payload = {"pcd_data": _SimplePCD(pc_data), "static_scene": False}
            else:
                payload = {"pcd_data": None, "static_scene": False}
            yield self.raw_topic, payload, frame_ts
