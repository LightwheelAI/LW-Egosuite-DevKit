"""
MCAP reader that yields decoded proto messages, optionally filtered by topic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, BinaryIO, Iterator, Tuple, Union

import subprocess


@dataclass
class DecodedProtoMessage:
    """One decoded message from an MCAP file."""

    topic: str
    log_time_ns: int
    publish_time_ns: int
    schema: Any
    channel: Any
    message: Any  # The decoded proto message (with Header + payload)


def iter_messages(
    path_or_stream: Union[str, BinaryIO],
    topics: list[str] | None = None,
) -> Iterator[DecodedProtoMessage]:
    """
    Iterate decoded proto messages in time order.
    If topics is provided, only yield messages for those topics.
    """
    from mcap.reader import make_reader
    from mcap_protobuf.decoder import DecoderFactory

    def _iter(src):
        reader = make_reader(src, decoder_factories=[DecoderFactory()])
        for schema, channel, raw_msg, proto_msg in reader.iter_decoded_messages(topics=topics):
            yield DecodedProtoMessage(
                topic=channel.topic,
                log_time_ns=raw_msg.log_time,
                publish_time_ns=raw_msg.publish_time,
                schema=schema,
                channel=channel,
                message=proto_msg,
            )

    if isinstance(path_or_stream, str):
        with open(path_or_stream, "rb") as f:
            yield from _iter(f)
    else:
        yield from _iter(path_or_stream)


class EgosuiteMcapReader:
    """Context manager that opens an MCAP file and provides iter_messages over it."""

    def __init__(self, path_or_stream: Union[str, BinaryIO]) -> None:
        self._path_or_stream = path_or_stream
        self._file = None

    def __enter__(self) -> "EgosuiteMcapReader":
        if isinstance(self._path_or_stream, str):
            self._file = open(self._path_or_stream, "rb")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def iter_messages(self, topics: list[str] | None = None) -> Iterator[DecodedProtoMessage]:
        """Iterate decoded messages; optionally filter by topics."""
        source = self._file if self._file is not None else self._path_or_stream
        return iter_messages(source, topics=topics)

    def iter_video_frames(
        self,
        topic: str,
        *,
        output: str = "numpy",
        device: str | None = None,
        queue_size: int = 10,
    ) -> Iterator[Any]:
        """
        Stream-decode a camera video topic (foxglove.CompressedVideo) into frames.

        This function:
          - Reads MCAP messages for the given topic in order.
          - Starts a single ffmpeg process and feeds it H.264 data incrementally.
          - Uses a small in-memory queue to buffer decoded frames (backpressure if slow consumer).
          - Ensures that every CompressedVideo message produces exactly one frame,
            or raises a RuntimeError.

        It yields:
          - numpy.ndarray with shape (H, W, 3), dtype=uint8 if output == "numpy"
          - torch.Tensor with shape (3, H, W), dtype=uint8 if output == "torch"
        """
        if output not in ("numpy", "torch"):
            raise ValueError(f"Unsupported output type {output!r}; expected 'numpy' or 'torch'.")
        if queue_size <= 0:
            raise ValueError("queue_size must be a positive integer.")

        # Lazily import numpy / torch only if needed.
        try:
            import numpy as np  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - import error handling
            raise ImportError(
                "numpy is required to decode foxglove.CompressedVideo frames. "
                "Please install numpy (e.g. `pip install numpy`)."
            ) from exc

        torch = None
        if output == "torch":
            try:
                import torch as _torch  # type: ignore[import-not-found]
            except ImportError as exc:
                raise ImportError(
                    "torch is required when output='torch'. "
                    "Please install PyTorch (e.g. `pip install torch`)."
                ) from exc
            torch = _torch

        import threading
        from queue import Queue

        source = self._file if self._file is not None else self._path_or_stream

        # Shared state between threads
        width_height: list[int | None] = [None, None]  # [width, height]
        proc_holder: list[Any] = [None]
        total_in: list[int] = [0]
        total_decoded: list[int] = [0]
        error_holder: list[BaseException | None] = [None]

        ready_event = threading.Event()
        frames_q: "Queue[Any]" = Queue(maxsize=queue_size)
        sentinel = object()

        def _read_exact(stream, n: int) -> bytes:
            """Read exactly n bytes from a blocking stream, or fewer if EOF."""
            buf = bytearray()
            while len(buf) < n:
                chunk = stream.read(n - len(buf))
                if not chunk:
                    break
                buf.extend(chunk)
            return bytes(buf)

        def producer() -> None:
            """Read MCAP messages and feed H.264 data into ffmpeg stdin."""
            proc = None
            try:
                for m in iter_messages(source, topics=[topic]):
                    data = getattr(m.message, "data", None)
                    if not isinstance(data, (bytes, bytearray)):
                        continue
                    data = bytes(data)

                    # Initialize ffmpeg process and discover frame size on first frame.
                    if width_height[0] is None or width_height[1] is None:
                        try:
                            w, h = _probe_h264_frame_size(data)
                        except BaseException as e:  # noqa: BLE001
                            error_holder[0] = e
                            break
                        width_height[0], width_height[1] = w, h
                        try:
                            proc = subprocess.Popen(
                                [
                                    "ffmpeg",
                                    "-loglevel",
                                    "error",
                                    "-f",
                                    "h264",
                                    "-i",
                                    "-",
                                    "-f",
                                    "rawvideo",
                                    "-pix_fmt",
                                    "rgb24",
                                    "-",
                                ],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                            )
                        except FileNotFoundError:
                            error_holder[0] = RuntimeError(
                                "ffmpeg is required to decode foxglove.CompressedVideo frames, "
                                "but it was not found on PATH."
                            )
                            break
                        proc_holder[0] = proc
                        ready_event.set()

                    proc = proc_holder[0]
                    if proc is None or proc.stdin is None:
                        error_holder[0] = RuntimeError(
                            "Internal error: ffmpeg process is not initialized."
                        )
                        break

                    try:
                        proc.stdin.write(data)
                        proc.stdin.flush()
                    except BaseException as e:  # noqa: BLE001
                        error_holder[0] = e
                        break

                    total_in[0] += 1
            finally:
                proc = proc_holder[0]
                if proc is not None and proc.stdin is not None:
                    try:
                        proc.stdin.close()
                    except OSError:
                        pass

        def consumer() -> None:
            """Read decoded rawvideo frames from ffmpeg stdout into a bounded queue."""
            ready_event.wait()
            proc = proc_holder[0]
            w, h = width_height
            if proc is None or proc.stdout is None or w is None or h is None:
                frames_q.put(sentinel)
                return

            frame_size = int(w) * int(h) * 3

            try:
                while True:
                    chunk = _read_exact(proc.stdout, frame_size)
                    if not chunk or len(chunk) < frame_size:
                        break
                    frame_np = np.frombuffer(chunk, dtype=np.uint8).reshape((h, w, 3))
                    total_decoded[0] += 1
                    frames_q.put(frame_np)
            except BaseException as e:  # noqa: BLE001
                error_holder[0] = e
            finally:
                frames_q.put(sentinel)
                if proc is not None:
                    proc.wait()

        # Start background producer/consumer threads.
        t_prod = threading.Thread(target=producer, daemon=True)
        t_cons = threading.Thread(target=consumer, daemon=True)
        t_prod.start()
        t_cons.start()

        try:
            # Main generator loop: block when queue is empty or full.
            while True:
                frame_np = frames_q.get()
                if frame_np is sentinel:
                    break
                if output == "numpy":
                    yield frame_np
                else:
                    assert torch is not None  # for type checkers
                    frame_t = torch.from_numpy(frame_np).permute(2, 0, 1).contiguous()
                    if device is not None:
                        frame_t = frame_t.to(device)
                    yield frame_t
        finally:
            # Ensure background threads and ffmpeg process are cleaned up.
            t_prod.join()
            t_cons.join()
            proc = proc_holder[0]
            if proc is not None:
                try:
                    if proc.stdout is not None:
                        proc.stdout.close()
                except OSError:
                    pass
                if proc.poll() is None:
                    try:
                        proc.kill()
                    except OSError:
                        pass

        # Propagate any error that occurred in producer/consumer threads.
        if error_holder[0] is not None:
            raise error_holder[0]

        # Strict check: every CompressedVideo message must produce exactly one frame.
        if total_in[0] != total_decoded[0]:
            raise RuntimeError(
                f"Decoded {total_decoded[0]} frame(s) from topic {topic!r}, "
                f"but MCAP contains {total_in[0]} CompressedVideo message(s)."
            )


def _probe_h264_frame_size(data: bytes) -> Tuple[int, int]:
    """
    Use ffprobe to infer (width, height) for a single H.264 Annex B frame.

    Expects ffprobe to be available on PATH.
    """
    try:
        r = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-f",
                "h264",
                "-i",
                "-",
                "-show_entries",
                "stream=width,height",
                "-of",
                "csv=p=0",
            ],
            input=data,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "ffprobe is required to decode foxglove.CompressedVideo frames, "
            "but it was not found on PATH."
        ) from exc

    if r.returncode != 0 or not r.stdout:
        raise RuntimeError(f"ffprobe failed to read H.264 frame size (returncode={r.returncode}).")

    stdout_txt = r.stdout.decode("utf-8", errors="ignore").strip()
    if not stdout_txt:
        raise RuntimeError("ffprobe did not return any width/height information.")

    line = stdout_txt.splitlines()[0]
    parts = line.split(",")
    if len(parts) != 2:
        raise RuntimeError(f"Unexpected ffprobe output while reading frame size: {line!r}")

    width = int(parts[0].strip())
    height = int(parts[1].strip())
    if width <= 0 or height <= 0:
        raise RuntimeError(f"Invalid frame size from ffprobe: {width}x{height}")
    return width, height


def _decode_h264_frame_to_numpy(data: bytes, width: int, height: int):
    """
    Decode a single H.264 Annex B access unit into an RGB numpy array (H, W, 3).

    This uses ffmpeg via subprocess and requires numpy to be installed.
    """
    try:
        import numpy as np  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - import error handling
        raise ImportError(
            "numpy is required to decode foxglove.CompressedVideo frames. "
            "Please install numpy (e.g. `pip install numpy`)."
        ) from exc

    try:
        r = subprocess.run(
            [
                "ffmpeg",
                "-loglevel",
                "error",
                "-f",
                "h264",
                "-i",
                "-",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "-",
            ],
            input=data,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "ffmpeg is required to decode foxglove.CompressedVideo frames, "
            "but it was not found on PATH."
        ) from exc

    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg failed to decode H.264 frame (returncode={r.returncode}).")

    raw = r.stdout
    expected_size = width * height * 3
    if len(raw) != expected_size:
        raise RuntimeError(
            f"Unexpected decoded frame size: got {len(raw)} bytes, "
            f"expected {expected_size} for frame {width}x{height}."
        )

    frame = (
        np.frombuffer(raw, dtype=np.uint8)  # type: ignore[name-defined]
        .reshape((height, width, 3))
    )
    return frame


def iter_video_frames(
    path_or_stream: Union[str, BinaryIO],
    topic: str,
    *,
    output: str = "numpy",
    device: str | None = None,
) -> Iterator[Any]:
    """
    Convenience wrapper around EgosuiteMcapReader.iter_video_frames() for a single topic.
    """
    with EgosuiteMcapReader(path_or_stream) as r:
        yield from r.iter_video_frames(topic, output=output, device=device)
