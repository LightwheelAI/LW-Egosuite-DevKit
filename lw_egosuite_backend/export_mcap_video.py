"""
Export a foxglove.CompressedVideo topic from an MCAP file to MP4.

Usage:
  lw-egosuite export-video --mcap path/to/file.mcap --output output.mp4 [--topic /sensor/camera/head_left/video]
  python -m lw_egosuite_backend.export_mcap_video path/to/file.mcap --topic /sensor/camera/head_left/video -o output.mp4

Requires ffmpeg on PATH. Uses stream copy (no re-encode). Output is a valid MP4 with
moov atom at the start (-movflags +faststart) for compatibility.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from lw_egosuite_backend.mcap_reader import iter_messages


@dataclass
class ExportMcapVideo:
    """Export a CompressedVideo topic from an MCAP file to MP4."""

    mcap: Path
    """Input MCAP file path."""

    output: Path
    """Output MP4 file path."""

    topic: str = "/sensor/camera/head_left/video"
    """CompressedVideo topic to export (e.g. /sensor/camera/head_left/video)."""

    def run(self) -> int:
        return export_video_run(self.mcap, self.topic, self.output)


def export_video_run(mcap_path: Path, topic: str, output_mp4: Path) -> int:
    """Extract CompressedVideo messages from topic and write to MP4 via ffmpeg. Returns 0 on success, 1 on error."""
    mcap_path = mcap_path.resolve()
    output_mp4 = output_mp4.resolve()

    if not mcap_path.exists():
        print(f"Error: MCAP file not found: {mcap_path}", file=sys.stderr)
        return 1

    frame_count = 0
    proc = None

    try:
        # Write to temp file first (piped H.264 -> MP4 puts moov at end).
        fd, tmp_name = tempfile.mkstemp(suffix=".mp4")
        os.close(fd)
        tmp_path = Path(tmp_name)

        proc = subprocess.Popen(
            [
                "ffmpeg",
                "-y",
                "-loglevel", "error",
                "-f", "h264",
                "-i", "-",
                "-c", "copy",
                str(tmp_path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        assert proc.stdin is not None

        with open(mcap_path, "rb") as f:
            for m in iter_messages(f, topics=[topic]):
                data = getattr(m.message, "data", None)
                if not isinstance(data, (bytes, bytearray)):
                    continue
                proc.stdin.write(bytes(data))
                frame_count += 1

        proc.stdin.close()
        proc.stdin = None

        _, stderr = proc.communicate(timeout=120)
        if proc.returncode != 0:
            tmp_path.unlink(missing_ok=True)
            msg = stderr.decode("utf-8", errors="replace").strip() if stderr else "unknown"
            print(f"Error: ffmpeg failed (code {proc.returncode}): {msg}", file=sys.stderr)
            return 1
        proc = None

        if frame_count == 0:
            tmp_path.unlink(missing_ok=True)
            print(f"Error: no CompressedVideo messages found on topic {topic!r}", file=sys.stderr)
            return 1

        # Remux with faststart so moov is at the beginning (valid, playable everywhere).
        r = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-loglevel", "error",
                "-i", str(tmp_path),
                "-c", "copy",
                "-movflags", "+faststart",
                "-f", "mp4",
                str(output_mp4),
            ],
            capture_output=True,
            timeout=60,
        )
        tmp_path.unlink(missing_ok=True)
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", errors="replace").strip() if r.stderr else "unknown"
            print(f"Error: ffmpeg faststart failed (code {r.returncode}): {err}", file=sys.stderr)
            return 1

        print(f"Exported {frame_count} frame(s) to {output_mp4}")
        return 0

    except FileNotFoundError:
        print(
            "Error: ffmpeg not found. Please install ffmpeg and ensure it is on PATH.",
            file=sys.stderr,
        )
        return 1
    except subprocess.TimeoutExpired:
        if proc is not None:
            proc.kill()
            proc.wait()
        print("Error: ffmpeg timed out", file=sys.stderr)
        return 1
    finally:
        if proc is not None and proc.stdin is not None:
            try:
                proc.stdin.close()
            except OSError:
                pass
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
                proc.wait()
            except OSError:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a CompressedVideo topic from MCAP to MP4"
    )
    parser.add_argument("mcap", type=Path, help="Input MCAP file path")
    parser.add_argument(
        "--topic",
        "-t",
        type=str,
        default="/sensor/camera/head_left/video",
        help="CompressedVideo topic (default: /sensor/camera/head_left/video)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Output MP4 file path",
    )
    args = parser.parse_args()
    sys.exit(export_video_run(args.mcap, args.topic, args.output))


if __name__ == "__main__":
    main()
