import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple

from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory

from foxglove_backend.base.base_reader import BaseReader

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


def _build_pose_frames(file_path: Path) -> Dict[int, Dict[str, Any]]:
    topics = [
        "/pose/body",
        "/pose/left_hand",
        "/pose/right_hand",
        "/pose/head_pose",
        "/pose/headcam_pose",
        "/pose/right_eye_cam",
        "/pose/pelvis",
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
                "pelvis": None,
            },
        )

        if topic == "/pose/pelvis":
            tf_obj = getattr(msg, "transform", None)
            if tf_obj is not None:
                frame["pelvis"] = _tf_to_dict(tf_obj)
            continue

        transforms = list(getattr(msg, "transforms", []))
        tf_dicts = [_tf_to_dict(tf_obj) for tf_obj in transforms]

        if topic == "/pose/body":
            frame["body"] = tf_dicts
            if tf_dicts and frame["pelvis"] is None:
                frame["pelvis"] = tf_dicts[0]
        elif topic == "/pose/left_hand":
            frame["left_hand"] = tf_dicts
        elif topic == "/pose/right_hand":
            frame["right_hand"] = tf_dicts
        elif topic == "/pose/head_pose":
            frame["head_pose"] = tf_dicts[0] if tf_dicts else None
        elif topic == "/pose/headcam_pose":
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
            body_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in (frame.get("body") or [])
            ]
            left_hand_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in (frame.get("left_hand") or [])
            ]
            right_hand_points = [
                {"x": p["x"], "y": p["y"], "z": p["z"]}
                for p in (frame.get("right_hand") or [])
            ]

            head_pose = frame.get("head_pose")
            if head_pose is None:
                if body_points:
                    head_idx = 15 if len(body_points) > 15 else 0
                    head_pose = body_points[head_idx]
                else:
                    logger.warning("Head pose is None, using default value (0.0, 0.0, 0.0, w=1.0, x=0.0, y=0.0, z=0.0).")
                    head_pose = {"x": 0.0, "y": 0.0, "z": 0.0, "quat": {"w": 1.0, "x": 0.0, "y": 0.0, "z": 0.0}}
            if len(body_points) > 15:
                # fix head 15
                body_points[15] = head_pose


            headcam_pose = frame.get("headcam_pose") or head_pose
            right_eye_cam_pose = frame.get("right_eye_cam_pose") or head_pose
            pelvis_pose = frame.get("pelvis") or {
                "x": 0.0,
                "y": 0.0,
                "z": 0.0,
                "quat": {"w": 1.0, "x": 0.0, "y": 0.0, "z": 0.0},
            }

            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000

            frame_packet = {
                "timestamp": int(ts),
                "timestamp_obj": {"seconds": int(sec), "nanos": int(nanos)},
                "pelvis_pose": {
                    "position": {
                        "x": pelvis_pose.get("x", 0.0),
                        "y": pelvis_pose.get("y", 0.0),
                        "z": pelvis_pose.get("z", 0.0),
                    },
                    "orientation": pelvis_pose.get(
                        "quat", {"w": 1.0, "x": 0.0, "y": 0.0, "z": 0.0}
                    ),
                },
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
            if head_pose is None:
                body_points = frame.get("body") or []
                if body_points:
                    head_idx = 15 if len(body_points) > 15 else 0
                    head_pose = body_points[head_idx]
                else:
                    head_pose = {"x": 0.0, "y": 0.0, "z": 0.0}

            current_head = {
                "x": float(head_pose.get("x", 0.0)),
                "y": float(head_pose.get("y", 0.0)),
                "z": float(head_pose.get("z", 0.0)),
            }
            trajectory_points.append(current_head)
            sec = ts // 1_000_000_000
            nanos = ts % 1_000_000_000
            msg = {
                "trajectory_points": trajectory_points[-min(len(trajectory_points), self.points_number_to_show):],
                "current_head": current_head,
                "timestamp_obj": {"seconds": int(sec), "nanos": int(nanos)},
            }
            yield self.raw_topic, msg, int(ts)


@dataclass(kw_only=True)
class StdPoseTFReader(BaseReader):
    pose_data_reader: StdPoseDataReader

    def generate_line(self) -> Generator[Tuple[str, Any, int], Any, None]:
        for ts, frame in self.pose_data_reader.load_frames().items():
            tf_data: List[Dict[str, Any]] = []

            pelvis = frame.get("pelvis")
            if pelvis is not None:
                tf_data.append(
                    {
                        "parent_frame_id": "world",
                        "child_frame_id": "pelvis",
                        "translation": {
                            "x": pelvis["x"],
                            "y": pelvis["y"],
                            "z": pelvis["z"],
                        },
                        "rotation": pelvis["quat"],
                    }
                )

            for idx, body_tf in enumerate(frame.get("body") or []):
                if idx >= len(BODY_FRAME_NAMES):
                    break
                child = BODY_FRAME_NAMES[idx]
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

