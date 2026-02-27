from . import Generator, register
from typing import Tuple, Dict
from pathlib import Path
import torch
from google.protobuf.timestamp_pb2 import Timestamp


@register("subtask-annotation")
@register("*/subtask_annotation")
class AnnotationsGenerator(Generator):

    @property
    def outputs(self) -> Dict[str, str]:
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""
        return dict(
            {f"{prefix}/subtask_annotation": "lightwheel.SubtaskAnnotation"},
            **{f"{prefix}/annotation_image_annotations": "foxglove.ImageAnnotations"}
        )

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        self._stem = parts[0] if parts and not self.src_topic.startswith(
            "/") else None
        self.subtask_annotation_cls = self.get_message_type(
            "lightwheel.SubtaskAnnotation")
        self.image_annotations_cls = self.get_message_type(
            "foxglove.ImageAnnotations")

    def generate(self, data: dict, timestamp):
        """
        Generate annotations from input data
        :param data: Dictionary containing annotation information
        :param timestamp: Timestamp
        """
        prefix = f"/{self._stem}" if getattr(self, "_stem", None) else ""

        # Process lightwheel.SubtaskAnnotation message
        subtask_msg = self.subtask_annotation_cls()
        image_annotations_msg = self.image_annotations_cls()

        # Process annotation data
        if isinstance(data, dict):
            description = data.get("description", {})
            has_annotation = data.get("has_annotation", False)

            # Set timestamp
            if "timestamp_seconds" in data and "timestamp_nanos" in data:
                sec = int(data.get("timestamp_seconds", 0))
                nanos = int(data.get("timestamp_nanos", 0))
            else:
                sec = int(timestamp // 1_000_000_000)
                nanos = int(timestamp % 1_000_000_000)

            # Process lightwheel.SubtaskAnnotation
            # skill = data.get("description", "")
            # if description and skill:
            # subtask_msg.data = skill + ": " + description
            subtask_msg.data = str(description)
            subtask_msg.timestamp.seconds = sec
            subtask_msg.timestamp.nanos = nanos

            if type(description) == dict:
                annotation_dict = description
                annotation_text = annotation_dict.get("description", "")
                annotation_text += "\n"
                annotation_text += "skill: " + annotation_dict.get("skill", "")
            else:
                annotation_text = str(description)
            # Process foxglove.ImageAnnotations (similar to low_quality.py)
            if has_annotation and description:
                text = image_annotations_msg.texts.add()
                text.timestamp.seconds = sec
                text.timestamp.nanos = nanos
                text.position.x = 60
                text.position.y = 160
                text.text = annotation_text
                text.font_size = 50
                text.text_color.r = 255 / 255.0
                text.text_color.g = 255 / 255.0
                text.text_color.b = 255 / 255.0
                text.text_color.a = 1
                text.background_color.r = 40 / 255.0
                text.background_color.g = 40 / 255.0
                text.background_color.b = 40 / 255.0
                text.background_color.a = 1

        # Yield both messages
        yield f"{prefix}/subtask_annotation", subtask_msg
        yield f"{prefix}/annotation_image_annotations", image_annotations_msg
