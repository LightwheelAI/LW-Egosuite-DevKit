import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from lw_egosuite_backend.base.base_reader import BaseReader

logger = logging.getLogger(__name__)
# Keep body/hand naming consistent with legacy pose TF conversion.
BODY_FRAME_NAMES = [
    "pelvis",          # 0
    "left_hip",        # 1
    "right_hip",       # 2
    "spine1",          # 3
    "left_knee",       # 4
    "right_knee",      # 5
    "spine2",          # 6
    "left_ankle",      # 7
    "right_ankle",     # 8
    "spine3",          # 9
    "left_foot",       # 10
    "right_foot",      # 11
    "neck",            # 12
    "left_collar",     # 13
    "right_collar",    # 14
    "head",            # 15
    "left_shoulder",   # 16
    "right_shoulder",  # 17
    "left_elbow",      # 18
    "right_elbow",     # 19
    # "left_wrist",      # 20 duplicate with hand, body tf don't use
    # "right_wrist",     # 21 duplicate with hand, body tf don't use
]

HAND_FRAME_NAMES = [
    "wrist",                  # 0
    "thumb_cmc",              # 1
    "thumb_mcp",              # 2
    "thumb_ip",               # 3
    "thumb_tip",              # 4
    "index_finger_mcp",       # 5
    "index_finger_pip",       # 6
    "index_finger_dip",       # 7
    "index_finger_tip",       # 8
    "middle_finger_mcp",      # 9
    "middle_finger_pip",      # 10
    "middle_finger_dip",      # 11
    "middle_finger_tip",      # 12
    "ring_finger_mcp",        # 13
    "ring_finger_pip",        # 14
    "ring_finger_dip",        # 15
    "ring_finger_tip",        # 16
    "pinky_mcp",              # 17
    "pinky_pip",              # 18
    "pinky_dip",              # 19
    "pinky_tip"               # 20
]


# Maps partial-body topic joint index → full 22-joint skeleton index.
# upper_body: 14 joints (pelvis + spine + head + arms)
UPPER_BODY_TO_FULL_IDX = [0, 3, 6, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
# lower_body: 8 joints (legs only, pelvis lives in upper_body)
LOWER_BODY_TO_FULL_IDX = [1, 2, 4, 5, 7, 8, 10, 11]

_ALL_BODY_TOPICS = ("/pose/body", "/pose/upper_body", "/pose/lower_body")


def _body_layout_map(n: int) -> List[int]:
    """Map a body array's local joint index -> full 22-joint skeleton index.

    The body array is stored in its native length: 22 (full /pose/body or a
    merged upper+lower), 14 (upper_body only) or 8 (lower_body only). This lets
    downstream consumers resolve the correct joint name / semantics regardless
    of which topic the data came from.
    """
    if n == 14:
        return UPPER_BODY_TO_FULL_IDX
    if n == 8:
        return LOWER_BODY_TO_FULL_IDX
    return list(range(n))  # full body: local index == full index


def _tf_to_dict(tf_obj: Any) -> Dict[str, Any]:
    pos = getattr(tf_obj, "pos", None)
    quat = getattr(tf_obj, "quat", None)
    return {
        "x": float(getattr(pos, "x", 0.0)),
        "y": float(getattr(pos, "y", 0.0)),
        "z": float(getattr(pos, "z", 0.0)),
        "quat": {
            "w": float(getattr(quat, "w", 1.0)),
            "x": float(getattr(quat, "x", 0.0)),
            "y": float(getattr(quat, "y", 0.0)),
            "z": float(getattr(quat, "z", 0.0)),
        },
    }


def _extract_decoded_item(item: Any) -> Tuple[str, int, Any]:
    if hasattr(item, "channel"):
        topic = item.channel.topic
        message = getattr(item, "message", None)
        ts = int(getattr(message, "log_time", 0))
        msg = getattr(item, "decoded_message", None) or message
        return topic, ts, msg
    try:
        _schema, channel, message, decoded_message = item  # type: ignore[misc]
        topic = getattr(channel, "topic", "")
        ts = int(getattr(message, "log_time", 0))
        msg = decoded_message or message
        return topic, ts, msg
    except ValueError:
        topic, msg, ts = item  # type: ignore[misc]
        return topic, int(ts), msg


def _select_body_topics(file_path: Path) -> List[str]:
    """Pick which body pose topic(s) to read from the MCAP.

    Priority: if /pose/body exists, use it. Otherwise fall back to whichever of
    /pose/upper_body and /pose/lower_body are present.
    """
    with file_path.open("rb") as f:
        summary = make_reader(f).get_summary()
    available = (
        {ch.topic for ch in summary.channels.values()}
        if summary is not None
        else set()
    )
    if "/pose/body" in available:
        return ["/pose/body"]
    partial = [
        t for t in ("/pose/upper_body", "/pose/lower_body") if t in available
    ]
    if partial:
        logger.info("No /pose/body found, using partial body topics: %s", partial)
        return partial
    logger.warning("No body pose topic found in %s", file_path)
    return ["/pose/body"]


def _scatter_into_full_body(
    frame: Dict[str, Any], tf_dicts: List[Dict[str, Any]], index_map: List[int]
) -> None:
    """Scatter a partial-body transform list into the merged 22-slot body array.

    Only used when both upper_body and lower_body are present and get combined
    into a single full skeleton.
    """
    body = frame["body"]
    if len(body) < 22:
        body = [None] * 22
        frame["body"] = body
    for local_idx, full_idx in enumerate(index_map):
        if local_idx < len(tf_dicts):
            body[full_idx] = tf_dicts[local_idx]


def _build_pose_frames(file_path: Path) -> Dict[int, Dict[str, Any]]:
    body_topics = _select_body_topics(file_path)
    # Merge into one full 22-joint skeleton only when both halves are present.
    # A single partial topic is kept in its native length (14 or 8) so we never
    # fabricate joints that were not in the source.
    merge_partial = {"/pose/upper_body", "/pose/lower_body"} <= set(body_topics)
    topics = [
        *body_topics,
        "/pose/left_hand",
        "/pose/right_hand",
        "/pose/head",
        "/pose/headcam",
        "/pose/right_eye_cam",
    ]
    reader = make_reader(file_path.open(
        "rb"), decoder_factories=[DecoderFactory()])

    frames: Dict[int, Dict[str, Any]] = {}
    for item in reader.iter_decoded_messages(topics=topics):
        topic, ts, msg = _extract_decoded_item(item)
        frame = frames.setdefault(
            ts,
            {
                "body": [],
                "left_hand": [],
                "right_hand": [],
                "head_pose": None,
                "headcam_pose": None,
                "right_eye_cam_pose": None,
            },
        )

        transforms = list(getattr(msg, "transforms", []))
        tf_dicts = [_tf_to_dict(tf_obj) for tf_obj in transforms]

        if topic == "/pose/body":
            frame["body"] = tf_dicts
        elif topic == "/pose/upper_body":
            if merge_partial:
                _scatter_into_full_body(frame, tf_dicts, UPPER_BODY_TO_FULL_IDX)
            else:
                frame["body"] = tf_dicts  # native 14-joint layout
        elif topic == "/pose/lower_body":
            if merge_partial:
                _scatter_into_full_body(frame, tf_dicts, LOWER_BODY_TO_FULL_IDX)
            else:
                frame["body"] = tf_dicts  # native 8-joint layout
        elif topic == "/pose/left_hand":
            frame["left_hand"] = tf_dicts
        elif topic == "/pose/right_hand":
            frame["right_hand"] = tf_dicts
        elif topic == "/pose/head":
            frame["head_pose"] = tf_dicts[0] if tf_dicts else None
        elif topic == "/pose/headcam":
            frame["headcam_pose"] = tf_dicts[0] if tf_dicts else None
        elif topic == "/pose/right_eye_cam":
            frame["right_eye_cam_pose"] = tf_dicts[0] if tf_dicts else None

    return dict(sorted(frames.items(), key=lambda kv: kv[0]))


@dataclass(kw_only=True)
class StdPoseDataReader:
    file_path: Path
    _frames_cache: Dict[int, Dict[str, Any]] = None

    def load_frames(self) -> Dict[int, Dict[str, Any]]:
        if self._frames_cache is None:
            self._frames_cache = _build_pose_frames(self.file_path)
        return self._frames_cache


@dataclass(kw_only=True)
class StdPoseSceneReader(BaseReader):
    pose_data_reader: StdPoseDataReader

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        for ts, frame in self.pose_data_reader.load_frames().items():
            raw_body = frame.get("body") or []
            # Body is stored in its native length (22 / 14 / 8). The scene
            # visualiser picks the matching bone topology from this length, so
            # we pass the points through as-is without padding.
            body_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in raw_body
                if p is not None
            ]
            left_hand_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in (frame.get("left_hand") or [])
            ]
            right_hand_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in (frame.get("right_hand") or [])
            ]

            # head_pose is an independent tracking topic; keep it separate from
            # the body's own head joint (they are different physical points).
            head_pose = frame.get("head_pose") or {
                "x": 0.0, "y": 0.0, "z": 0.0,
                "quat": {"w": 1.0, "x": 0.0, "y": 0.0, "z": 0.0},
            }

            headcam_pose = frame.get("headcam_pose") or head_pose
            right_eye_cam_pose = frame.get("right_eye_cam_pose") or head_pose

            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000

            frame_packet = {
                "timestamp": int(ts),
                "timestamp_obj": {"seconds": int(sec), "nanos": int(nanos)},
                "head_pose": {
                    "x": head_pose.get("x", 0.0),
                    "y": head_pose.get("y", 0.0),
                    "z": head_pose.get("z", 0.0),
                },
                "headcam_pose": {
                    "x": headcam_pose.get("x", 0.0),
                    "y": headcam_pose.get("y", 0.0),
                    "z": headcam_pose.get("z", 0.0),
                },
                "right_eye_cam_pose": {
                    "x": right_eye_cam_pose.get("x", 0.0),
                    "y": right_eye_cam_pose.get("y", 0.0),
                    "z": right_eye_cam_pose.get("z", 0.0),
                },
                "joints": {
                    "body": body_points,
                    "left_hand": left_hand_points,
                    "right_hand": right_hand_points,
                },
            }

            yield self.raw_topic, frame_packet, int(ts)


@dataclass(kw_only=True)
class StdHeadPoseTrajectoryReader(BaseReader):
    pose_data_reader: StdPoseDataReader
    points_number_to_show: int

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        trajectory_points: List[Dict[str, float]] = []
        for ts, frame in self.pose_data_reader.load_frames().items():
            head_pose = frame.get("head_pose")
            current_head = None
            if head_pose:
                current_head = {
                    "x": float(head_pose.get("x", 0.0)),
                    "y": float(head_pose.get("y", 0.0)),
                    "z": float(head_pose.get("z", 0.0)),
                }
                trajectory_points.append(current_head)

            # Body head joint (full-skeleton index 15) for the head↔body-head line
            body = frame.get("body") or []
            layout = _body_layout_map(len(body))
            body_head_local = next((i for i, fi in enumerate(layout) if fi == 15), None)
            body_head = None
            if body_head_local is not None and body_head_local < len(body) and body[body_head_local]:
                p = body[body_head_local]
                body_head = {"x": float(p["x"]), "y": float(p["y"]), "z": float(p["z"])}

            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000
            msg = {
                "trajectory_points": trajectory_points[-min(len(trajectory_points), self.points_number_to_show):],
                "current_head": current_head,
                "current_body_head": body_head,
                "current_headcam": frame.get("headcam_pose"),
                "current_right_eye_cam": frame.get("right_eye_cam_pose"),
                "timestamp_obj": {"seconds": int(sec), "nanos": int(nanos)},
            }
            yield self.raw_topic, msg, int(ts)


@dataclass(kw_only=True)
class StdFootTrajectoryReader(BaseReader):
    """Emit left/right foot trajectory frames; yields nothing if lower body joints are absent."""

    pose_data_reader: StdPoseDataReader
    points_number_to_show: int

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        frames = self.pose_data_reader.load_frames()
        if not frames:
            return

        # Detect lower body availability once from the first frame.
        # Feet are full-skeleton indices 10 (left_foot) and 11 (right_foot).
        first_body = next(iter(frames.values())).get("body") or []
        layout = _body_layout_map(len(first_body))
        left_local = next((i for i, fi in enumerate(layout) if fi == 10), None)
        right_local = next((i for i, fi in enumerate(layout) if fi == 11), None)
        if left_local is None and right_local is None:
            logger.info("No lower body joints in this MCAP, skipping foot trajectory.")
            return

        left_traj: List[Dict[str, float]] = []
        right_traj: List[Dict[str, float]] = []

        for ts, frame in frames.items():
            body = frame.get("body") or []

            def _pt(local_idx):
                if local_idx is None or local_idx >= len(body):
                    return None
                p = body[local_idx]
                if p is None:
                    return None
                return {"x": float(p["x"]), "y": float(p["y"]), "z": float(p["z"])}

            left_pt = _pt(left_local)
            right_pt = _pt(right_local)
            if left_pt:
                left_traj.append(left_pt)
            if right_pt:
                right_traj.append(right_pt)

            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000
            msg = {
                "left_trajectory": left_traj[-self.points_number_to_show:],
                "right_trajectory": right_traj[-self.points_number_to_show:],
                "current_left": left_traj[-1] if left_traj else None,
                "current_right": right_traj[-1] if right_traj else None,
                "timestamp_obj": {"seconds": int(sec), "nanos": int(nanos)},
            }
            yield self.raw_topic, msg, int(ts)


@dataclass(kw_only=True)
class StdPoseTFReader(BaseReader):
    pose_data_reader: StdPoseDataReader

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        for ts, frame in self.pose_data_reader.load_frames().items():
            tf_data: List[Dict[str, Any]] = []

            body = frame.get("body") or []
            # Resolve each stored joint to its full-skeleton index so the frame
            # name is correct regardless of layout (full 22 / upper 14 / lower 8).
            layout = _body_layout_map(len(body))
            for idx, body_tf in enumerate(body):
                if body_tf is None:
                    continue
                full_idx = layout[idx]
                if full_idx >= len(BODY_FRAME_NAMES):
                    continue
                child = BODY_FRAME_NAMES[full_idx]
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": child,
                        "translation": {
                            "x": body_tf["x"],
                            "y": body_tf["y"],
                            "z": body_tf["z"],
                        },
                        "rotation": body_tf["quat"],
                    }
                )

            for idx, hand_tf in enumerate(frame.get("left_hand") or []):
                if idx >= len(HAND_FRAME_NAMES):
                    break
                child = f"left_{HAND_FRAME_NAMES[idx]}"
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": child,
                        "translation": {
                            "x": hand_tf["x"],
                            "y": hand_tf["y"],
                            "z": hand_tf["z"],
                        },
                        "rotation": hand_tf["quat"],
                    }
                )

            for idx, hand_tf in enumerate(frame.get("right_hand") or []):
                if idx >= len(HAND_FRAME_NAMES):
                    break
                child = f"right_{HAND_FRAME_NAMES[idx]}"
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": child,
                        "translation": {
                            "x": hand_tf["x"],
                            "y": hand_tf["y"],
                            "z": hand_tf["z"],
                        },
                        "rotation": hand_tf["quat"],
                    }
                )

            if frame.get("head_pose") is not None:
                t = frame["head_pose"]
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": "head_pose",
                        "translation": {"x": t["x"], "y": t["y"], "z": t["z"]},
                        "rotation": t["quat"],
                    }
                )

            if frame.get("headcam_pose") is not None:
                t = frame["headcam_pose"]
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": "head_left_camera",
                        "translation": {"x": t["x"], "y": t["y"], "z": t["z"]},
                        "rotation": t["quat"],
                    }
                )

            if frame.get("right_eye_cam_pose") is not None:
                t = frame["right_eye_cam_pose"]
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": "head_right_camera",
                        "translation": {"x": t["x"], "y": t["y"], "z": t["z"]},
                        "rotation": t["quat"],
                    }
                )

            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000
            yield self.raw_topic, {
                "timestamp_seconds": int(sec),
                "timestamp_nanos": int(nanos),
                "tf_data": tf_data,
            }, int(ts)
