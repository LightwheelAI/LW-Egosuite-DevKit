from . import Generator, register
from typing import Dict

# --- Topology Definition (Index Pairs) ---
# Full 22-joint body (indices refer to raw body points before appending head/cam):
#   0=pelvis, 1=left_hip, 2=right_hip, 3=spine1, 4=left_knee, 5=right_knee,
#   6=spine2, 7=left_ankle, 8=right_ankle, 9=spine3, 10=left_foot, 11=right_foot,
#   12=neck, 13=left_collar, 14=right_collar, 15=head, 16=left_shoulder,
#   17=right_shoulder, 18=left_elbow, 19=right_elbow, 20=left_wrist, 21=right_wrist
# After appending: 22=headcam_pose, 23=right_eye_cam_pose
UPPER_BODY_BONES_22 = [
    (0, 3), (3, 6), (6, 9), (9, 12), (12, 15),    # Spine
    (9, 13), (13, 16), (16, 18), (18, 20),         # Left arm
    (9, 14), (14, 17), (17, 19), (19, 21),         # Right arm
]

LOWER_BODY_BONES_22 = [
    (0, 1), (1, 4), (4, 7), (7, 10),              # Left leg
    (0, 2), (2, 5), (5, 8), (8, 11),              # Right leg
]

# 14-joint upper-body only (lower limbs 1,2,4,5,7,8,10,11 removed):
#   0=pelvis, 1=spine1, 2=spine2, 3=spine3, 4=neck, 5=left_collar,
#   6=right_collar, 7=head, 8=left_shoulder, 9=right_shoulder,
#   10=left_elbow, 11=right_elbow, 12=left_wrist, 13=right_wrist
# After appending: 14=headcam_pose, 15=right_eye_cam_pose
UPPER_BODY_BONES_14 = [
    (0, 1), (1, 2), (2, 3), (3, 4), (4, 7),      # Spine
    (3, 5), (5, 8), (8, 10), (10, 12),            # Left arm
    (3, 6), (6, 9), (9, 11), (11, 13),            # Right arm
]

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
            f"{prefix}/upper_body_keypoints": "foxglove.SceneUpdate",
            f"{prefix}/lower_body_keypoints": "foxglove.SceneUpdate",
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
            is_22 = len(raw_body_pts) > 14
            upper_bones = UPPER_BODY_BONES_22 if is_22 else UPPER_BODY_BONES_14
            lower_bones = LOWER_BODY_BONES_22

            upper_topic = f"/{self._stem}/upper_body_keypoints" if self._stem else "/upper_body_keypoints"
            lower_topic = f"/{self._stem}/lower_body_keypoints" if self._stem else "/lower_body_keypoints"

            upper_indices = sorted(
                {idx for pair in upper_bones for idx in pair if idx <
                    len(world_body_pts)}
            )
            update_msg_upper, entity_upper = create_base_entity(
                "upper_body_skeleton")
            add_spheres(entity_upper, [world_body_pts[i]
                        for i in upper_indices], 0.022, COLOR_JOINT)
            add_lines(entity_upper, world_body_pts,
                      upper_bones, 0.01, COLOR_BODY)
            yield upper_topic, update_msg_upper

            if is_22:
                lower_indices = sorted(
                    {idx for pair in lower_bones for idx in pair if idx <
                        len(world_body_pts)}
                )
                update_msg_lower, entity_lower = create_base_entity(
                    "lower_body_skeleton")
                add_spheres(entity_lower, [world_body_pts[i]
                            for i in lower_indices], 0.022, COLOR_JOINT)
                add_lines(entity_lower, world_body_pts,
                          lower_bones, 0.01, COLOR_BODY)
                yield lower_topic, update_msg_lower

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


@register("/head-trajectory")
@register("*/head_trajectory")
class HeadPoseTrajectoryGenerator(Generator):
    """Convert head trajectory to foxglove.SceneUpdate: trajectory line (LINE_STRIP) + current-frame head sphere marker, for display in Foxglove 3D view."""

    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        return {f"{prefix}/head_trajectory": "foxglove.SceneUpdate"}

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
        current_body_head = data.get("current_body_head")
        timestamp_obj = data.get("timestamp_obj", {})
        if not current_head:
            return

        update_msg = self.scene_update_cls()
        entity = update_msg.entities.add()
        entity.id = "head_trajectory"
        entity.frame_id = "world"
        entity.timestamp.seconds = timestamp_obj.get("seconds", 0)
        entity.timestamp.nanos = timestamp_obj.get("nanos", 0)
        entity.lifetime.seconds = 0
        entity.lifetime.nanos = 0  # 0 means permanent display, never disappears
        entity.frame_locked = True

        # Trajectory dots: jet colormap (blue→cyan→green→yellow→orange→red)
        # oldest = blue, newest = red; older dots are more transparent
        n = len(trajectory_points)
        for i, pt in enumerate(trajectory_points):
            if not (isinstance(pt, dict) and "x" in pt and "y" in pt and "z" in pt):
                continue
            t = i / max(n - 1, 1)  # 0.0 = oldest, 1.0 = newest
            r = min(max(1.5 - abs(4 * t - 3), 0.0), 1.0)
            g = min(max(1.5 - abs(4 * t - 2), 0.0), 1.0)
            b = min(max(1.5 - abs(4 * t - 1), 0.0), 1.0)
            sphere = entity.spheres.add()
            sphere.pose.position.x = float(pt["x"])
            sphere.pose.position.y = float(pt["y"])
            sphere.pose.position.z = float(pt["z"])
            sphere.pose.orientation.w = 1.0
            dot_size = 0.012 + 0.008 * t
            sphere.size.x = dot_size
            sphere.size.y = dot_size
            sphere.size.z = dot_size
            sphere.color.r = r
            sphere.color.g = g
            sphere.color.b = b
            sphere.color.a = 0.3 + 0.7 * t

        # Current frame head sphere (yellow)
        if isinstance(current_head, dict) and "x" in current_head:
            sphere = entity.spheres.add()
            sphere.pose.position.x = float(current_head["x"])
            sphere.pose.position.y = float(current_head["y"])
            sphere.pose.position.z = float(current_head["z"])
            sphere.pose.orientation.w = 1.0
            sphere.size.x = sphere.size.y = sphere.size.z = 0.04
            sphere.color.r = 1.0
            sphere.color.g = 0.85
            sphere.color.b = 0.0
            sphere.color.a = 1.0

        yield f"/{self._stem}/head_trajectory" if self._stem else "/head_trajectory", update_msg


@register("/foot-trajectory")
@register("*/foot_trajectory")
class FootTrajectoryGenerator(Generator):
    """Foot trajectory: jet colormap dots for left and right foot."""

    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        return {f"{prefix}/foot_trajectory": "foxglove.SceneUpdate"}

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        self._stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        self.scene_update_cls = self.get_message_type("foxglove.SceneUpdate")

    def generate(self, data, timestamp):
        if not isinstance(data, dict):
            return
        left_traj = data.get("left_trajectory") or []
        right_traj = data.get("right_trajectory") or []
        current_left = data.get("current_left")
        current_right = data.get("current_right")
        timestamp_obj = data.get("timestamp_obj", {})

        if not (left_traj or right_traj):
            return

        update_msg = self.scene_update_cls()
        entity = update_msg.entities.add()
        entity.id = "foot_trajectory"
        entity.frame_id = "world"
        entity.timestamp.seconds = timestamp_obj.get("seconds", 0)
        entity.timestamp.nanos = timestamp_obj.get("nanos", 0)
        entity.lifetime.seconds = 0
        entity.lifetime.nanos = 0
        entity.frame_locked = True

        def _jet_dots(points, dot_base_size):
            n = len(points)
            for i, pt in enumerate(points):
                if not (isinstance(pt, dict) and "x" in pt and "y" in pt and "z" in pt):
                    continue
                t = i / max(n - 1, 1)
                r = min(max(1.5 - abs(4 * t - 3), 0.0), 1.0)
                g = min(max(1.5 - abs(4 * t - 2), 0.0), 1.0)
                b = min(max(1.5 - abs(4 * t - 1), 0.0), 1.0)
                sphere = entity.spheres.add()
                sphere.pose.position.x = float(pt["x"])
                sphere.pose.position.y = float(pt["y"])
                sphere.pose.position.z = float(pt["z"])
                sphere.pose.orientation.w = 1.0
                s = dot_base_size + 0.006 * t
                sphere.size.x = s
                sphere.size.y = s
                sphere.size.z = s
                sphere.color.r = r
                sphere.color.g = g
                sphere.color.b = b
                sphere.color.a = 0.3 + 0.7 * t

        _jet_dots(left_traj, 0.010)
        _jet_dots(right_traj, 0.010)

        # Current left foot sphere (larger, blue)
        if isinstance(current_left, dict) and "x" in current_left:
            sphere = entity.spheres.add()
            sphere.pose.position.x = float(current_left["x"])
            sphere.pose.position.y = float(current_left["y"])
            sphere.pose.position.z = float(current_left["z"])
            sphere.pose.orientation.w = 1.0
            sphere.size.x = sphere.size.y = sphere.size.z = 0.035
            sphere.color.r = 0.2
            sphere.color.g = 0.4
            sphere.color.b = 1.0
            sphere.color.a = 1.0

        # Current right foot sphere (larger, pink)
        if isinstance(current_right, dict) and "x" in current_right:
            sphere = entity.spheres.add()
            sphere.pose.position.x = float(current_right["x"])
            sphere.pose.position.y = float(current_right["y"])
            sphere.pose.position.z = float(current_right["z"])
            sphere.pose.orientation.w = 1.0
            sphere.size.x = sphere.size.y = sphere.size.z = 0.035
            sphere.color.r = 1.0
            sphere.color.g = 0.4
            sphere.color.b = 0.7
            sphere.color.a = 1.0

        yield f"/{self._stem}/foot_trajectory" if self._stem else "/foot_trajectory", update_msg
