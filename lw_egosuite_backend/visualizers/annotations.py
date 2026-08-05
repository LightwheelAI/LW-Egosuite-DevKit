from . import Generator, register
from typing import Any, Dict, List


def _wrap_line(line: str, max_chars: int) -> str:
    line = (line or "").strip()
    if not line or max_chars <= 0:
        return line
    if len(line) <= max_chars:
        return line

    words = line.split()
    if not words:
        # No whitespace; hard wrap.
        return "\n".join(line[i: i + max_chars] for i in range(0, len(line), max_chars))

    out_lines: List[str] = []
    cur: List[str] = []
    cur_len = 0

    for w in words:
        # Hard-wrap very long tokens.
        if len(w) > max_chars:
            if cur:
                out_lines.append(" ".join(cur))
                cur = []
                cur_len = 0
            out_lines.extend(w[i: i + max_chars]
                             for i in range(0, len(w), max_chars))
            continue

        add_len = len(w) if not cur else (1 + len(w))
        if cur_len + add_len <= max_chars:
            cur.append(w)
            cur_len += add_len
        else:
            out_lines.append(" ".join(cur))
            cur = [w]
            cur_len = len(w)

    if cur:
        out_lines.append(" ".join(cur))
    return "\n".join(out_lines)


def _wrap_text(text: str, max_chars_per_line: int) -> str:
    # Preserve existing newlines; wrap each line separately.
    lines = (text or "").splitlines() or [""]
    return "\n".join(_wrap_line(line, max_chars_per_line) for line in lines).strip()


def _add_wrapped_text(
    image_annotations_msg: Any,
    *,
    sec: int,
    nanos: int,
    x: float,
    base_y: float,
    text: str,
    font_size: float,
    line_spacing: float,
    text_rgba: tuple,
    bg_rgba: tuple,
) -> None:
    """
    Add wrapped text as multiple TextAnnotations (one line each).
    This guarantees the first line stays at base_y across renderers.
    """
    lines = (text or "").splitlines() or [""]
    line_h = float(font_size) * float(line_spacing)
    for i, line in enumerate(lines):
        t = image_annotations_msg.texts.add()
        t.timestamp.seconds = sec
        t.timestamp.nanos = nanos
        t.position.x = float(x)
        t.position.y = float(base_y) + i * line_h
        t.text = line
        t.font_size = float(font_size)
        tr, tg, tb, ta = text_rgba
        br, bg, bb, ba = bg_rgba
        t.text_color.r = tr
        t.text_color.g = tg
        t.text_color.b = tb
        t.text_color.a = ta
        t.background_color.r = br
        t.background_color.g = bg
        t.background_color.b = bb
        t.background_color.a = ba


@register("subtask-annotation")
class AnnotationsGenerator(Generator):

    @property
    def outputs(self) -> Dict[str, str]:
        return {
            "/state-transitions/subtask_description": "foxglove.Log",
            "/image-annotations/semantic_segments": "foxglove.ImageAnnotations",
        }

    def setup(self, **kwargs):
        self.log_cls = self.get_message_type("foxglove.Log")
        self.image_annotations_cls = self.get_message_type(
            "foxglove.ImageAnnotations")

    def generate(self, data: dict, timestamp):
        """
        Generate annotations from input data
        :param data: Dictionary containing annotation information
        :param timestamp: Timestamp
        """
        desc_msg = self.log_cls()
        image_annotations_msg = self.image_annotations_cls()

        has_desc = False
        has_caption = False
        has_skill = False
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

            if type(description) == dict:
                annotation_dict = description
                desc_part = (annotation_dict.get("description") or "").strip()
                skill_part = (annotation_dict.get("skill") or "").strip()
                caption_part = (annotation_dict.get("caption") or "").strip()
                desc_line = desc_part
            else:
                desc_line = str(description).strip() if description else ""
                skill_part = ""
                caption_part = ""

            has_desc = bool(desc_line and has_annotation)
            has_caption = bool(caption_part)
            has_skill = bool(skill_part and has_annotation)

            # Fill foxglove.Log message; append skill after the description if present
            log_text = desc_line
            if has_skill:
                log_text = f"{log_text} (skill: {skill_part})" if log_text else f"skill: {skill_part}"
            desc_msg.timestamp.seconds = sec
            desc_msg.timestamp.nanos = nanos
            desc_msg.message = log_text
            desc_msg.level = 2  # INFO
            MARGIN_X = 40
            CAPTION_Y = 70
            CAPTION_FONT_SIZE = 36
            CAPTION_LINE_SPACING = 1.0
            DESC_FONT_SIZE = 46
            DESC_GAP = 20
            SKILL_FONT_SIZE = 42
            IMAGE_HEIGHT = 1456
            SKILL_Y = IMAGE_HEIGHT - (CAPTION_Y - CAPTION_FONT_SIZE)
            if has_caption:
                wrapped_cap = _wrap_text(caption_part, max_chars_per_line=80)
                _add_wrapped_text(
                    image_annotations_msg,
                    sec=sec,
                    nanos=nanos,
                    x=MARGIN_X,
                    base_y=CAPTION_Y,
                    text=wrapped_cap,
                    font_size=CAPTION_FONT_SIZE,
                    line_spacing=CAPTION_LINE_SPACING,
                    text_rgba=(1.0, 1.0, 1.0, 1.0),
                    bg_rgba=(40 / 255.0, 40 / 255.0, 40 / 255.0, 0.8),
                )
                caption_line_count = max(1, len(wrapped_cap.splitlines()))
                caption_bottom_y = CAPTION_Y + caption_line_count * \
                    (CAPTION_FONT_SIZE * CAPTION_LINE_SPACING)
            else:
                caption_bottom_y = CAPTION_Y
            if has_desc:
                wrapped = _wrap_text(desc_line, max_chars_per_line=62)
                desc_base_y = caption_bottom_y + DESC_GAP if has_caption else CAPTION_Y
                _add_wrapped_text(
                    image_annotations_msg,
                    sec=sec,
                    nanos=nanos,
                    x=MARGIN_X,
                    base_y=desc_base_y,
                    text=wrapped,
                    font_size=DESC_FONT_SIZE,
                    line_spacing=1.0,
                    text_rgba=(1.0, 1.0, 1.0, 1.0),
                    bg_rgba=(40 / 255.0, 40 / 255.0, 40 / 255.0, 0.8),
                )
            if has_skill:
                _add_wrapped_text(
                    image_annotations_msg,
                    sec=sec,
                    nanos=nanos,
                    x=MARGIN_X,
                    base_y=SKILL_Y,
                    text=f"skill: {skill_part}",
                    font_size=SKILL_FONT_SIZE,
                    line_spacing=1.0,
                    text_rgba=(1.0, 1.0, 1.0, 1.0),
                    bg_rgba=(40 / 255.0, 40 / 255.0, 40 / 255.0, 0.8),
                )

        yield "/state-transitions/subtask_description", desc_msg
        if has_desc or has_caption or has_skill:
            yield "/image-annotations/semantic_segments", image_annotations_msg
