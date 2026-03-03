from . import Generator, register
from typing import Dict


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

        has_desc = False
        has_caption = False
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

            subtask_msg.data = str(description)
            subtask_msg.timestamp.seconds = sec
            subtask_msg.timestamp.nanos = nanos

            if type(description) == dict:
                annotation_dict = description
                desc_part = (annotation_dict.get("description") or "").strip()
                skill_part = (annotation_dict.get("skill") or "").strip()
                caption_part = (annotation_dict.get("caption") or "").strip()
                desc_line = desc_part
                if skill_part:
                    desc_line += ("\n" if desc_line else "") + "skill: " + skill_part
            else:
                desc_line = str(description).strip() if description else ""
                caption_part = ""

            # caption: always top line, fixed y so it never moves. desc: fixed y below caption.
            has_desc = bool(desc_line and has_annotation)
            has_caption = bool(caption_part)
            CAPTION_Y = 80
            DESC_Y = 150
            if has_caption:
                cap = image_annotations_msg.texts.add()
                cap.timestamp.seconds = sec
                cap.timestamp.nanos = nanos
                cap.position.x = 50
                cap.position.y = CAPTION_Y
                cap.text = caption_part
                cap.font_size = 36
                cap.text_color.r = 255 / 255.0
                cap.text_color.g = 255 / 255.0
                cap.text_color.b = 255 / 255.0
                cap.text_color.a = 1
                cap.background_color.r = 40 / 255.0
                cap.background_color.g = 40 / 255.0
                cap.background_color.b = 40 / 255.0
                cap.background_color.a = 1
            if has_desc:
                text = image_annotations_msg.texts.add()
                text.timestamp.seconds = sec
                text.timestamp.nanos = nanos
                text.position.x = 50
                text.position.y = DESC_Y
                text.text = desc_line
                text.font_size = 50
                text.text_color.r = 255 / 255.0
                text.text_color.g = 255 / 255.0
                text.text_color.b = 255 / 255.0
                text.text_color.a = 1
                text.background_color.r = 40 / 255.0
                text.background_color.g = 40 / 255.0
                text.background_color.b = 40 / 255.0
                text.background_color.a = 1

        yield f"{prefix}/subtask_annotation", subtask_msg
        if has_desc or has_caption:
            yield f"{prefix}/annotation_image_annotations", image_annotations_msg
