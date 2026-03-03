from . import Generator, register
from typing import Dict


@register("/Audio")
@register("*/audio")
class AudioGenerator(Generator):
    """Convert wav block payloads to foxglove.RawAudio."""

    @property
    def outputs(self) -> Dict[str, str]:
        parts = self.src_topic.split("/")
        stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        prefix = f"/{stem}" if stem else ""
        return {
            f"{prefix}/audio": "foxglove.RawAudio"
        }

    def setup(self, **kwargs):
        parts = self.src_topic.split("/")
        stem = parts[0] if parts and not self.src_topic.startswith("/") else None
        prefix = f"/{stem}" if stem else ""
        self.output_topic_name = f"{prefix}/audio"
        self.audio_cls = self.get_message_type("foxglove.RawAudio")

    def generate(self, data, timestamp):
        if not isinstance(data, dict):
            return
        msg = self.audio_cls()
        self.set_builtins_time(msg.timestamp, int(timestamp))
        msg.data = data.get("data", b"")
        msg.format = data.get("format", "pcm-s16")
        msg.sample_rate = int(data.get("sample_rate", 0))
        msg.number_of_channels = int(data.get("number_of_channels", 0))
        yield self.output_topic_name, msg
