from . import Generator, register
from typing import Dict

# --- Topology Definition (Index Pairs) ---
# Full 22-joint body (indices refer to raw body points before appending head/cam):
#   0=pelvis, 1=left_hip, 2=right_hip, 3=spine1, 4=left_knee, 5=right_knee,
#   6=spine2, 7=left_ankle, 8=right_ankle, 9=spine3, 10=left_foot, 11=right_foot,
#   12=neck, 13=left_collar, 14=right_collar, 15=head, 16=left_shoulder,
#   17=right_shoulder, 18=left_elbow, 19=right_elbow, 20=left_wrist, 21=right_wrist
# After appending: 22=headcam_pose, 23=right_eye_cam_pose
BODY_BONES_22 = [
    (0, 1), (1, 4), (4, 7), (7, 10),              # Left leg
    (0, 2), (2, 5), (5, 8), (8, 11),              # Right leg
    (0, 3), (3, 6), (6, 9), (9, 12), (12, 15),    # Spine
    (9, 13), (13, 16), (16, 18), (18, 20),         # Left arm
    (9, 14), (14, 17), (17, 19), (19, 21),         # Right arm
    (15, 22), (15, 23),                             # head->cam
]

# 14-joint upper-body only (lower limbs 1,2,4,5,7,8,10,11 removed):
#   0=pelvis, 1=spine1, 2=spine2, 3=spine3, 4=neck, 5=left_collar,
#   6=right_collar, 7=head, 8=left_shoulder, 9=right_shoulder,
#   10=left_elbow, 11=right_elbow, 12=left_wrist, 13=right_wrist
# After appending: 14=headcam_pose, 15=right_eye_cam_pose
BODY_BONES_14 = [
    (0, 1), (1, 2), (2, 3), (3, 4), (4, 7),      # Spine
    (3, 5), (5, 8), (8, 10), (10, 12),            # Left arm
    (3, 6), (6, 9), (9, 11), (11, 13),            # Right arm
    (7, 14), (7, 15),                               # head->cam
]


def _get_body_bones(num_body_joints: int):
    if num_body_joints <= 14:
        return BODY_BONES_14
    return BODY_BONES_22

HAND_BONES = [
    (0, 1), (1, 2), (2, 3), (3, 4),       # Thumb
    (0, 5), (5, 6), (6, 7), (7, 8),       # Index finger
    (0, 9), (9, 10), (10, 11), (11, 12),  # Middle finger
    (0, 13), (13, 14), (14, 15), (15, 16),  # Ring finger
    (0, 17), (17, 18), (18, 19), (19, 20)  # Pinky
]

# Color definitions
COLOR_BODY = {"r": 1.0, "g": 0.2, "b": 0.2, "a": 1.0}
COLOR_L_HAND = {"r": 0.2, "g": 0.2, "b": 1.0, "a": 1.0}
COLOR_R_HAND = {"r": 1.0, "g": 0.4, "b": 0.7, "a": 1.0}
COLOR_JOINT = {"r": 0.6, "g": 0.4, "b": 0.2, "a": 1.0}
COLOR_HAND_POINTS = {"r": 1.0, "g": 1.0, "b": 0, "a": 1.0}



@register("scene-update")
@register("*/scene_update")
class SceneUpdateGenerator(Generator):
    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        return {
            f"{prefix}/body_keypoints": "foxglove.SceneUpdate",
            f"{prefix}/right_hand_keypoints": "foxglove.SceneUpdate",
            f"{prefix}/left_hand_keypoints": "foxglove.SceneUpdate",
            f"{prefix}/right_hand_keypoints_2d": "foxglove.SceneUpdate",
            f"{prefix}/left_hand_keypoints_2d": "foxglove.SceneUpdate",
        }

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        self._stem = parts[0] if parts and not self.src_topic.startswith(
            "/") else None
        self.scene_update_cls = self.get_message_type("foxglove.SceneUpdate")
        # Even though the Primitive class is not explicitly required here, ensure the foxglove library is properly loaded.

    def generate(self, data, timestamp):
        raw_body_pts = data["joints"]["body"]
        body_bones = _get_body_bones(len(raw_body_pts))
        world_body_pts = list(raw_body_pts)
        world_body_pts.append(data["headcam_pose"])
        world_body_pts.append(data["right_eye_cam_pose"])

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
            used_indices = sorted(
                {idx for pair in body_bones for idx in pair if idx <
                    len(world_body_pts)}
            )
            body_pts = [world_body_pts[i] for i in used_indices]

            update_msg_body, entity_body = create_base_entity(
                "full_body_skeleton")
            add_spheres(entity_body, body_pts, 0.022, COLOR_JOINT)
            add_lines(entity_body, world_body_pts,
                      body_bones, 0.01, COLOR_BODY)

            yield f"/{self._stem}/body_keypoints" if self._stem else "/body_keypoints", update_msg_body

        # 3. Generate right-hand keypoints topic (/right_hand_keypoints)
        if len(world_r_hand_pts) > 0:
            update_msg_r_hand, entity_r_hand = create_base_entity(
                "right_hand_skeleton")
            add_spheres(entity_r_hand, world_r_hand_pts, 0.015, COLOR_JOINT)
            add_lines(entity_r_hand, world_r_hand_pts,
                      HAND_BONES, 0.005, COLOR_R_HAND)
            yield f"/{self._stem}/right_hand_keypoints" if self._stem else "/right_hand_keypoints", update_msg_r_hand

            update_msg_r_hand, entity_r_hand = create_base_entity(
                "right_hand_skeleton")
            add_spheres(entity_r_hand, world_r_hand_pts,
                        0.008, COLOR_HAND_POINTS)
            add_lines(entity_r_hand, world_r_hand_pts,
                      HAND_BONES, 0.0035, COLOR_R_HAND)
            yield f"/{self._stem}/right_hand_keypoints_2d" if self._stem else "/right_hand_keypoints_2d", update_msg_r_hand

        # 4. Generate left-hand keypoints topic (/left_hand_keypoints)
        if len(world_l_hand_pts) > 0:
            update_msg_l_hand, entity_l_hand = create_base_entity(
                "left_hand_skeleton")
            add_spheres(entity_l_hand, world_l_hand_pts, 0.015, COLOR_JOINT)
            add_lines(entity_l_hand, world_l_hand_pts,
                      HAND_BONES, 0.005, COLOR_L_HAND)
            yield f"/{self._stem}/left_hand_keypoints" if self._stem else "/left_hand_keypoints", update_msg_l_hand

            update_msg_l_hand, entity_l_hand = create_base_entity(
                "left_hand_skeleton")
            add_spheres(entity_l_hand, world_l_hand_pts,
                        0.008, COLOR_HAND_POINTS)
            add_lines(entity_l_hand, world_l_hand_pts,
                      HAND_BONES, 0.0035, COLOR_L_HAND)
            yield f"/{self._stem}/left_hand_keypoints_2d" if self._stem else "/left_hand_keypoints_2d", update_msg_l_hand


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
        self._stem = parts[0] if parts and not self.src_topic.startswith(
            "/") else None
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
