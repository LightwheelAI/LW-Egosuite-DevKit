from . import Generator, register
from typing import Dict
import json


@register("tf-tree")
@register("*/tf_tree")
class FrameTFGenerator(Generator):

    @property
    def outputs(self) -> Dict[str, str]:
        return dict({
            self.output_topic_name: "foxglove.FrameTransforms"
        })

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        stem = parts[0] if parts and not self.src_topic.startswith(
            "/") else None
        prefix = f"/{stem}" if stem else ""
        self.output_topic_name = f"{prefix}/tf_tree"
        # Handle human interaction data initialization
        self.frame_transforms_cls = self.get_message_type(
            "foxglove.FrameTransforms")

    def generate(self, data, timestamp):
        res_msg = self.frame_transforms_cls()

        # Process annotation data
        if isinstance(data, dict):
            tf_data = data.get("tf_data", {})
            for tf in tf_data:
                transform = res_msg.transforms.add()
                transform.parent_frame_id = tf["parent_frame_id"]
                transform.child_frame_id = tf["child_frame_id"]
                transform.translation.x = tf["translation"]["x"]
                transform.translation.y = tf["translation"]["y"]
                transform.translation.z = tf["translation"]["z"]
                transform.rotation.x = tf["rotation"]["x"]
                transform.rotation.y = tf["rotation"]["y"]
                transform.rotation.z = tf["rotation"]["z"]
                transform.rotation.w = tf["rotation"]["w"]

                transform.timestamp.seconds = int(data["timestamp_seconds"])
                transform.timestamp.nanos = int(data["timestamp_nanos"])

        yield self.output_topic_name, res_msg
