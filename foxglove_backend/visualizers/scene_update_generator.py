from . import Generator, register
from typing import Dict
import math
from scipy.spatial.transform import Rotation as R
import numpy as np

# --- Topology Definition (Index Pairs) ---
# Lower-body skeleton: left leg and right leg
LOWER_BODY_BONES = [
    (0, 1), (1, 4), (4, 7), (7, 10), # Left leg
    (0, 2), (2, 5), (5, 8), (8, 11), # Right Leg
]

# Upper-body skeleton: spine and arms + cam body的手腕单独画了
UPPER_BODY_BONES = [
    (0, 3), (3, 6), (6, 9), (9, 12), (12, 15), # Spine
    (9, 13), (13, 16), (16, 18), (18, 20), # Left Arm
    (9, 14), (14, 17), (17, 19), (19, 21),  # Right Arm
    (15, 22), (15, 23) # head->cam
]

# Keep the original BODY_BONES for compatibility (if needed)
BODY_BONES = LOWER_BODY_BONES + UPPER_BODY_BONES

HAND_BONES = [
    (0, 1), (1, 2), (2, 3), (3, 4),       # Thumb
    (0, 5), (5, 6), (6, 7), (7, 8),       # Index finger
    (0, 9), (9, 10), (10, 11), (11, 12),  # Middle finger
    (0, 13), (13, 14), (14, 15), (15, 16),# Ring finger
    (0, 17), (17, 18), (18, 19), (19, 20) # Pinky
]

# Color definitions
COLOR_BODY = {"r": 1.0, "g": 0.2, "b": 0.2, "a": 1.0}
COLOR_L_HAND = {"r": 0.2, "g": 0.2, "b": 1.0, "a": 1.0}
COLOR_R_HAND = {"r": 1.0, "g": 0.4, "b": 0.7, "a": 1.0}
COLOR_JOINT = {"r": 0.6, "g": 0.4, "b": 0.2, "a": 1.0}
COLOR_HAND_POINTS = {"r": 1.0, "g": 1.0, "b": 0, "a": 1.0}

# Whether to output only one complete body (True, default) or keep upper and lower body separated (False)
ONLY_FULL_BODY = True

@register("scene-update")
@register("*/scene_update")
class SceneUpdateGenerator(Generator):
    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        if ONLY_FULL_BODY:
            # Output only one full body + left and right hands
            return dict(
                { f"{prefix}/body_keypoints": "foxglove.SceneUpdate" },
                **{ f"{prefix}/right_hand_keyponts": "foxglove.SceneUpdate" },
                **{ f"{prefix}/left_hand_keyponts": "foxglove.SceneUpdate" },
                **{ f"{prefix}/right_hand_keyponts_2d": "foxglove.SceneUpdate" },
                **{ f"{prefix}/left_hand_keyponts_2d": "foxglove.SceneUpdate" },
            )
        else:
            # Upper and lower body separation + left and right hands
            return dict(
                { f"{prefix}/upper_body_keypoints": "foxglove.SceneUpdate" },
                **{ f"{prefix}/lower_body_keypoints": "foxglove.SceneUpdate" },
                **{ f"{prefix}/right_hand_keyponts": "foxglove.SceneUpdate" },
                **{ f"{prefix}/left_hand_keyponts": "foxglove.SceneUpdate" },
            )

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        self._stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        self.scene_update_cls = self.get_message_type("foxglove.SceneUpdate")
        # Even though the Primitive class is not explicitly required here, ensure the foxglove library is properly loaded.

    def generate(self, data, timestamp):
        # --- Core fix: Prepare coordinate transformation matrix ---
        # 1. Get the pelvis world pose
        # p_pose = data["pelvis_pose"]
        # pelvis_pos = np.array([p_pose["position"]["x"], p_pose["position"]["y"], p_pose["position"]["z"]])

        # 2. Construct rotation object (note: scipy order is x, y, z, w)
        world_body_pts = [p for p in data["joints"]["body"]]
        world_body_pts.append(data["headcam_pose"])
        world_body_pts.append(data["right_eye_cam_pose"])
        world_body_pts.append((data["headcam_pose"]))
        world_body_pts.append((data["right_eye_cam_pose"]))

        world_l_hand_pts = [p for p in data["joints"]["left_hand"]]
        world_r_hand_pts = [p for p in data["joints"]["right_hand"]]

        # --- Helper function: Create base entity ---
        def create_base_entity(entity_id):
            update_msg = self.scene_update_cls()
            entity = update_msg.entities.add()
            entity.id = entity_id
            entity.frame_id = "world"
            entity.timestamp.seconds = data["timestamp_obj"]["seconds"]
            entity.timestamp.nanos = data["timestamp_obj"]["nanos"]
            entity.lifetime.seconds = 0
            entity.lifetime.nanos = 100000000  # 100ms
            entity.frame_locked = True
            return update_msg, entity

        # --- Helper function: Add sphere ---
        def add_spheres(entity, points, size, color):
            for pt in points:
                sphere = entity.spheres.add()
                sphere.pose.position.x = pt["x"]
                sphere.pose.position.y = pt["y"]
                sphere.pose.position.z = pt["z"]
                sphere.pose.orientation.w = 1.0
                sphere.size.x = size
                sphere.size.y = size
                sphere.size.z = size
                sphere.color.r = color["r"]
                sphere.color.g = color["g"]
                sphere.color.b = color["b"]
                sphere.color.a = color["a"]

        # --- Helper function: Add line ---
        def add_lines(entity, points, connections, thickness, color):
            for start_idx, end_idx in connections:
                if start_idx >= len(points) or end_idx >= len(points):
                    continue

                line = entity.lines.add()
                line.type = 0  # LINE_STRIP
                line.thickness = thickness
                line.color.r = color["r"]
                line.color.g = color["g"]
                line.color.b = color["b"]
                line.color.a = color["a"]

                p_start = line.points.add()
                p_start.x = points[start_idx]["x"]
                p_start.y = points[start_idx]["y"]
                p_start.z = points[start_idx]["z"]

                p_end = line.points.add()
                p_end.x = points[end_idx]["x"]
                p_end.y = points[end_idx]["y"]
                p_end.z = points[end_idx]["z"]
        if len(world_body_pts) > 0:
            if ONLY_FULL_BODY:
                # 1. Full body: Use the BODY_BONES connectivity.
                used_indices = sorted(
                    {idx for pair in BODY_BONES for idx in pair if idx < len(world_body_pts)}
                )
                body_pts = [world_body_pts[i] for i in used_indices]

                update_msg_body, entity_body = create_base_entity("full_body_skeleton")
                add_spheres(entity_body, body_pts, 0.022, COLOR_JOINT)
                add_lines(entity_body, world_body_pts, BODY_BONES, 0.01, COLOR_BODY)

                yield f"/{self._stem}/body_keypoints" if self._stem else "/body_keypoints", update_msg_body

            else:
                # 1. Generate lower body keypoints topic (/lower_body_keypoints)
                # Includes: keypoints of the left and right legs (indices 0–11) and bones
                lower_body_indices = [0, 1, 2, 4, 5, 7, 8, 10, 11]  # Lower-body keypoint indices
                lower_body_pts = [world_body_pts[i] for i in lower_body_indices if i < len(world_body_pts)]

                update_msg_lower, entity_lower = create_base_entity("lower_body_skeleton")
                add_spheres(entity_lower, lower_body_pts, 0.008, COLOR_JOINT)
                add_lines(entity_lower, world_body_pts, LOWER_BODY_BONES, 0.0035, COLOR_BODY)
                yield f"/{self._stem}/lower_body_keypoints" if self._stem else "/lower_body_keypoints", update_msg_lower

                # 2. Generate upper body keypoints topic (/upper_body_keypoints)
                # Includes: key points of the spine and arms (indices 0, 3, 6, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23) and bones
                # Note: Index 0 (pelvis) is also included because the spine starts from the pelvis.
                upper_body_indices = [0, 3, 6, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]  # Upper body keypoint indices
                upper_body_pts = [world_body_pts[i] for i in upper_body_indices if i < len(world_body_pts)]

                update_msg_upper, entity_upper = create_base_entity("upper_body_skeleton")
                add_spheres(entity_upper, upper_body_pts, 0.022, COLOR_JOINT)
                add_lines(entity_upper, world_body_pts, UPPER_BODY_BONES, 0.01, COLOR_BODY)

                yield f"/{self._stem}/upper_body_keypoints" if self._stem else "/upper_body_keypoints", update_msg_upper

        # 3. Generate right-hand keypoints topic (/right_hand_keyponts)
        if len(world_r_hand_pts) > 0:
            update_msg_r_hand, entity_r_hand = create_base_entity("right_hand_skeleton")
            add_spheres(entity_r_hand, world_r_hand_pts, 0.015, COLOR_JOINT)
            add_lines(entity_r_hand, world_r_hand_pts, HAND_BONES, 0.005, COLOR_R_HAND)
            yield f"/{self._stem}/right_hand_keyponts" if self._stem else "/right_hand_keyponts", update_msg_r_hand

            update_msg_r_hand, entity_r_hand = create_base_entity("right_hand_skeleton")
            add_spheres(entity_r_hand, world_r_hand_pts, 0.008, COLOR_HAND_POINTS)
            add_lines(entity_r_hand, world_r_hand_pts, HAND_BONES, 0.0035, COLOR_R_HAND)
            yield f"/{self._stem}/right_hand_keyponts_2d" if self._stem else "/right_hand_keyponts_2d", update_msg_r_hand

        # 4. Generate left-hand keypoints topic (/left_hand_keypoints)
        if len(world_l_hand_pts) > 0:
            update_msg_l_hand, entity_l_hand = create_base_entity("left_hand_skeleton")
            add_spheres(entity_l_hand, world_l_hand_pts, 0.015, COLOR_JOINT)
            add_lines(entity_l_hand, world_l_hand_pts, HAND_BONES, 0.005, COLOR_L_HAND)
            yield f"/{self._stem}/left_hand_keyponts" if self._stem else "/left_hand_keyponts", update_msg_l_hand

            update_msg_l_hand, entity_l_hand = create_base_entity("left_hand_skeleton")
            add_spheres(entity_l_hand, world_l_hand_pts, 0.008, COLOR_HAND_POINTS)
            add_lines(entity_l_hand, world_l_hand_pts, HAND_BONES, 0.0035, COLOR_L_HAND)
            yield f"/{self._stem}/left_hand_keyponts_2d" if self._stem else "/left_hand_keyponts_2d", update_msg_l_hand


@register("/head-pose-trajectory")
@register("*/head_pose_trajectory")
class HeadPoseTrajectoryGenerator(Generator):
    """Convert head trajectory to foxglove.SceneUpdate: trajectory line (LINE_STRIP) + current-frame head sphere marker, for display in Foxglove 3D view."""

    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        return {f"{prefix}/head_pose_trajectory": "foxglove.SceneUpdate"}

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        self._stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        self.scene_update_cls = self.get_message_type("foxglove.SceneUpdate")

    def generate(self, data, timestamp):
        if not isinstance(data, dict):
            return
        trajectory_points = data.get("trajectory_points") or []
        current_head = data.get("current_head")
        timestamp_obj = data.get("timestamp_obj", {})
        if not current_head:
            return

        update_msg = self.scene_update_cls()
        entity = update_msg.entities.add()
        entity.id = "head_pose_trajectory"
        entity.frame_id = "world"
        entity.timestamp.seconds = timestamp_obj.get("seconds", 0)
        entity.timestamp.nanos = timestamp_obj.get("nanos", 0)
        entity.lifetime.seconds = 0
        entity.lifetime.nanos = 0  # 0 means permanent display, never disappears
        entity.frame_locked = True

        # Trajectory line: LINE_STRIP connecting head positions from frame 0 to the current frame
        # LINE_STRIP requires at least 2 points to be displayed

        if trajectory_points and len(trajectory_points) >= 2:
            line = entity.lines.add()
            line.type = 0  # LINE_STRIP
            line.thickness = 9
            line.scale_invariant = True
            line.color.r = 0.2
            line.color.g = 0.8
            line.color.b = 0.2
            line.color.a = 1.0
            for pt in trajectory_points:
                if isinstance(pt, dict) and "x" in pt and "y" in pt and "z" in pt:
                    p = line.points.add()
                    p.x = float(pt["x"])
                    p.y = float(pt["y"])
                    p.z = float(pt["z"])

        # Current frame head sphere marker (larger, yellow, indicating "current head")ead"）
        if isinstance(current_head, dict) and "x" in current_head and "y" in current_head and "z" in current_head:
            sphere = entity.spheres.add()
            sphere.pose.position.x = float(current_head["x"])
            sphere.pose.position.y = float(current_head["y"])
            sphere.pose.position.z = float(current_head["z"])
            sphere.pose.orientation.w = 1.0
            sphere.size.x = 0.04  # Larger sphere
            sphere.size.y = 0.04
            sphere.size.z = 0.04
            sphere.color.r = 1.0  # Yellow
            sphere.color.g = 0.85
            sphere.color.b = 0.0
            sphere.color.a = 1.0

        yield f"/{self._stem}/head_pose_trajectory" if self._stem else "/head_pose_trajectory", update_msg