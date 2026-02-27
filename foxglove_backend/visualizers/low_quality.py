from . import Generator, register
from typing import Dict


@register("low-quality-annotation")
@register("*/low_quality_annotation")
class LowQualityImageAnnotationsGenerator(Generator):
    """Convert low-quality frame labels to foxglove.ImageAnnotations."""

    @property
    def outputs(self) -> Dict[str, str]:
        return {self.output_topic_name: "foxglove.ImageAnnotations"}

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        prefix = f"/{stem}" if stem else ""
        self.output_topic_name = f"{prefix}/low_quality_annotations"
        self.annotations_cls = self.get_message_type("foxglove.ImageAnnotations")

    def generate(self, data: dict, timestamp):
        if not isinstance(data, dict):
            return

        problem_types = data.get("problem_types", []) or []
        if "timestamp_seconds" in data and "timestamp_nanos" in data:
            sec = int(data.get("timestamp_seconds", 0))
            nanos = int(data.get("timestamp_nanos", 0))
        else:
            sec = int(timestamp // 1_000_000_000)
            nanos = int(timestamp % 1_000_000_000)

        msg = self.annotations_cls()
        for idx, name in enumerate(problem_types):
            text = msg.texts.add()
            text.timestamp.seconds = sec
            text.timestamp.nanos = nanos
            text.position.x = 20.0
            text.position.y = 140 + idx * 100
            text.text = f"[low-quality] {name}"
            text.font_size = 90
            text.text_color.r = 1.0
            text.text_color.g = 0.2
            text.text_color.b = 0.2
            text.text_color.a = 0.6
            text.background_color.r = 0.0
            text.background_color.g = 0.0
            text.background_color.b = 0.0
            text.background_color.a = 0.55

        # Always publish a message. Empty `texts` clears previous annotations.
        yield self.output_topic_name, msg
