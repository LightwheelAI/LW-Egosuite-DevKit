import logging
from . import Generator, register
from typing import Dict, Any
import numpy as np
import struct

logger = logging.getLogger(__name__)


def _fill_pointcloud_header(msg: Any, secs: int, nsecs: int) -> None:
    """Fill timestamp, frame_id and identity pose on a foxglove.PointCloud message."""
    msg.timestamp.seconds = secs
    msg.timestamp.nanos = nsecs
    msg.frame_id = "world"
    msg.pose.position.x = 0.0
    msg.pose.position.y = 0.0
    msg.pose.position.z = 0.0
    msg.pose.orientation.w = 1.0
    msg.pose.orientation.x = 0.0
    msg.pose.orientation.y = 0.0
    msg.pose.orientation.z = 0.0


def _fill_pointcloud_from_pc_data(msg: Any, pcd_data: Any, packed_field_cls: type) -> None:
    """
    Fill msg.fields, msg.point_stride, msg.data from pcd_data.pc_data (structured array).
    If pcd_data is None or empty, set point_stride=0 and data=b\"\".
    """
    if pcd_data is None:
        msg.point_stride = 0
        msg.data = b""
        return
    points = pcd_data.pc_data
    if points.size == 0 or len(points) == 0:
        msg.point_stride = 0
        msg.data = b""
        return
    num_points = len(points)
    fields = []
    point_data_list = []
    offset = 0
    if hasattr(points.dtype, "names") and points.dtype.names:
        field_names = list(points.dtype.names)
        has_rgba = all(f in field_names for f in ["red", "green", "blue", "alpha"])
        for field_name in field_names:
            if field_name in ["x", "y", "z"]:
                f = packed_field_cls()
                f.name = field_name
                f.offset = offset
                f.type = packed_field_cls.FLOAT32
                fields.append(f)
                point_data_list.append(points[field_name].astype(np.float32))
                offset += 4
        if has_rgba:
            for name in ["red", "green", "blue", "alpha"]:
                f = packed_field_cls()
                f.name = name
                f.offset = offset
                f.type = packed_field_cls.UINT8
                fields.append(f)
                point_data_list.append(points[name].astype(np.uint8))
                offset += 1
    for field in fields:
        msg.fields.append(field)
    msg.point_stride = offset
    packed_data = bytearray(num_points * offset)
    for i in range(num_points):
        byte_offset = i * offset
        for j, field_data in enumerate(point_data_list):
            o = fields[j].offset
            value = field_data[i]
            if fields[j].type == packed_field_cls.FLOAT32:
                struct.pack_into("<f", packed_data, byte_offset + o, float(value))
            elif fields[j].type == packed_field_cls.UINT32:
                struct.pack_into("<I", packed_data, byte_offset + o, int(value))
            elif fields[j].type == packed_field_cls.UINT8:
                struct.pack_into("<B", packed_data, byte_offset + o, int(value))
    msg.data = bytes(packed_data)


@register("pointcloud/static")
class StdPCDStaticGenerator(Generator):
    """Generator to convert MCAP static point cloud data into foxglove.PointCloud messages."""

    @property
    def outputs(self) -> Dict[str, str]:
        return {self.output_topic_name: "foxglove.PointCloud"}

    def setup(self, **kwargs):
        self.output_topic_name = "/pointcloud/static"
        self.pointcloud_cls = self.get_message_type("foxglove.PointCloud")
        self.packed_field_cls = self.get_message_type("foxglove.PackedElementField")

    def generate(self, data, timestamp):
        if not isinstance(data, dict):
            return
        if not data.get("static_scene", False):
            return
        pcd_data = data.get("pcd_data")
        if pcd_data is None:
            return
        try:
            msg = self.pointcloud_cls()
            _fill_pointcloud_header(msg, *self.ns2sec_nsec(timestamp))
            _fill_pointcloud_from_pc_data(msg, pcd_data, self.packed_field_cls)
            yield self.output_topic_name, msg
        except Exception as e:
            logger.error("Error converting static point cloud data: %s", e)


@register("pointcloud/2d_projection")
class StdPCDPerFrameGenerator(Generator):
    """
    Per-frame point cloud: one message per frame; with data when available,
    empty PointCloud (0 points) otherwise so nothing is visible in the viewer.
    """

    @property
    def outputs(self) -> Dict[str, str]:
        return {self.output_topic_name: "foxglove.PointCloud"}

    def setup(self, **kwargs):
        self.output_topic_name = "/pointcloud/2d_projection"
        self.pointcloud_cls = self.get_message_type("foxglove.PointCloud")
        self.packed_field_cls = self.get_message_type("foxglove.PackedElementField")

    def generate(self, data, timestamp):
        if not isinstance(data, dict):
            return
        if data.get("static_scene", True):
            return
        msg = self.pointcloud_cls()
        _fill_pointcloud_header(msg, *self.ns2sec_nsec(timestamp))
        pcd_data = data.get("pcd_data")
        try:
            _fill_pointcloud_from_pc_data(msg, pcd_data, self.packed_field_cls)
            yield self.output_topic_name, msg
        except Exception as e:
            logger.error("Error converting per-frame point cloud: %s", e)
            msg.point_stride = 0
            msg.data = b""
            yield self.output_topic_name, msg
