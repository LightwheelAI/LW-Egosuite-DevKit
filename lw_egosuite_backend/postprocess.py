#!/usr/bin/env python3
"""
Post-processing for generated MCAP files.

Currently includes:
- Removing not-used (empty) topics: channels with 0 messages

Usage as CLI: postprocess.py <input.mcap> <output.mcap>
Usage as module: from postprocess import postprocess_mcap
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

from mcap.reader import make_reader
from mcap.writer import Writer


def postprocess_mcap(mcap_path: str) -> None:
    """
    Post-process an MCAP file in-place.

    Currently performs:
    - Remove not-used (empty) topics: channels with 0 messages

    Reads the MCAP, applies post-processing, and overwrites the original file.
    """
    input_path = Path(mcap_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Not a file: {input_path}")

    temp_path = input_path.with_suffix(".mcap.tmp")

    # First pass: count messages per channel
    channel_message_count: dict[int, int] = defaultdict(int)
    all_channels: dict[int, str] = {}
    with open(input_path, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        if summary and summary.channels:
            for cid, ch in summary.channels.items():
                all_channels[cid] = ch.topic
        for schema, channel, message in reader.iter_messages():
            channel_message_count[channel.id] += 1

    valid_channel_ids = {cid for cid, count in channel_message_count.items() if count > 0}
    empty_channel_ids = set(all_channels.keys()) - valid_channel_ids

    if not empty_channel_ids:
        return  # No empty topics, nothing to do

    # Second pass: write only non-empty channels to temp file
    with open(input_path, "rb") as f_in, open(temp_path, "wb") as f_out:
        reader = make_reader(f_in)
        writer = Writer(f_out)
        writer.start()

        schema_map: dict[int, int] = {}
        channel_map: dict[int, int] = {}

        for schema, channel, message in reader.iter_messages():
            if channel.id not in valid_channel_ids:
                continue

            if schema.id not in schema_map:
                new_schema_id = writer.register_schema(
                    schema.name,
                    schema.encoding,
                    schema.data,
                )
                schema_map[schema.id] = new_schema_id

            if channel.id not in channel_map:
                new_channel_id = writer.register_channel(
                    schema_id=schema_map[schema.id],
                    topic=channel.topic,
                    message_encoding=channel.message_encoding,
                    metadata=channel.metadata,
                )
                channel_map[channel.id] = new_channel_id

            writer.add_message(
                channel_id=channel_map[channel.id],
                log_time=message.log_time,
                publish_time=message.publish_time,
                data=message.data,
            )

        writer.finish()

    # Replace original with cleaned version
    temp_path.replace(input_path)


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: remove_empty_topics.py <input.mcap> <output.mcap>", file=sys.stderr)
        return 1
    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    if not input_path.is_file():
        print(f"Not a file: {input_path}", file=sys.stderr)
        return 1

    # First pass: count messages per channel, get all channels from summary
    channel_message_count: dict[int, int] = defaultdict(int)
    all_channels: dict[int, str] = {}  # channel_id -> topic
    with open(input_path, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        if summary and summary.channels:
            for cid, ch in summary.channels.items():
                all_channels[cid] = ch.topic
        for schema, channel, message in reader.iter_messages():
            channel_message_count[channel.id] += 1

    valid_channel_ids = {cid for cid, count in channel_message_count.items() if count > 0}
    empty_channel_ids = set(all_channels.keys()) - valid_channel_ids
    if empty_channel_ids:
        empty_topics = sorted({all_channels[cid] for cid in empty_channel_ids})
        print(f"Removing {len(empty_topics)} empty topic(s):")
        for t in empty_topics:
            print(f"  {t}")
    else:
        print("No empty topics (writing all channels to output).")

    # Second pass: write only non-empty channels to output
    with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
        reader = make_reader(f_in)
        writer = Writer(f_out)
        writer.start()

        schema_map: dict[int, int] = {}
        channel_map: dict[int, int] = {}

        for schema, channel, message in reader.iter_messages():
            if channel.id not in valid_channel_ids:
                continue

            if schema.id not in schema_map:
                new_schema_id = writer.register_schema(
                    schema.name,
                    schema.encoding,
                    schema.data,
                )
                schema_map[schema.id] = new_schema_id

            if channel.id not in channel_map:
                new_channel_id = writer.register_channel(
                    schema_id=schema_map[schema.id],
                    topic=channel.topic,
                    message_encoding=channel.message_encoding,
                    metadata=channel.metadata,
                )
                channel_map[channel.id] = new_channel_id

            writer.add_message(
                channel_id=channel_map[channel.id],
                log_time=message.log_time,
                publish_time=message.publish_time,
                data=message.data,
            )

        writer.finish()

    print("Remove empty topic done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
