import logging
from . import Generator, register
from typing import Dict
import numpy as np
import struct

logger = logging.getLogger(__name__)


@register("pointcloud/static")
class StdPCDStaticGenerator(Generator):
    """Generator to convert MCAP static point cloud data into foxglove.PointCloud messages"""

    @property
    def outputs(self) -> Dict[str, str]:
        return dict({
            self.output_topic_name: "foxglove.PointCloud"
        })

    def setup(self, **kwargs):
        self.output_topic_name = "/pointcloud/static"
        self.pointcloud_cls = self.get_message_type("foxglove.PointCloud")
        self.packed_field_cls = self.get_message_type(
            "foxglove.PackedElementField")

    def generate(self, data, timestamp):
        """Convert MCAP static point cloud data to foxglove.PointCloud message"""
        if not isinstance(data, dict):
            return

        # Check if this is a static scene point cloud
        is_static_scene = data.get('static_scene', False)
        if not is_static_scene:
            return

        pcd_data = data.get('pcd_data')
        if pcd_data is None:
            return

        # Create PointCloud message
        pointcloud_msg = self.pointcloud_cls()

        # Set timestamp
        secs, nsecs = self.ns2sec_nsec(timestamp)
        pointcloud_msg.timestamp.seconds = secs
        pointcloud_msg.timestamp.nanos = nsecs

        # Set frame_id
        pointcloud_msg.frame_id = "world"

        # Set default pose (identity matrix, no rotation or translation)
        pointcloud_msg.pose.position.x = 0.0
        pointcloud_msg.pose.position.y = 0.0
        pointcloud_msg.pose.position.z = 0.0
        pointcloud_msg.pose.orientation.w = 1.0
        pointcloud_msg.pose.orientation.x = 0.0
        pointcloud_msg.pose.orientation.y = 0.0
        pointcloud_msg.pose.orientation.z = 0.0

        # Get point cloud data
        try:
            # PointCloud object from pypcd4, using pc_data (structured array) instead of numpy()
            points = pcd_data.pc_data

            # Check point cloud data
            if points.size == 0:
                return

            num_points = len(points)
            if num_points == 0:
                return

            # Determine fields and data
            # pcd_data.pc_data is a structured array with field names.
            fields = []
            point_data_list = []
            offset = 0

            # Check whether it is a structured array (with field names).
            if hasattr(points.dtype, 'names') and points.dtype.names:
                # Structured array with named fields
                field_names = list(points.dtype.names)

                # Check if pre-converted r, g, b, a fields exist.
                has_rgba_preprocessed = all(
                    f in field_names for f in ['red', 'green', 'blue', 'alpha'])

                for field_name in field_names:
                    field_data = points[field_name]

                    if field_name in ['x', 'y', 'z']:
                        # Coordinate field, using FLOAT32
                        field = self.packed_field_cls()
                        field.name = field_name
                        field.offset = offset
                        field.type = self.packed_field_cls.FLOAT32
                        fields.append(field)
                        point_data_list.append(field_data.astype(np.float32))
                        offset += 4

                if has_rgba_preprocessed:
                    # Add color fields
                    for name, key in [('red', 'red'), ('green', 'green'), ('blue', 'blue'), ('alpha', 'alpha')]:
                        field = self.packed_field_cls()
                        field.name = name
                        field.offset = offset
                        field.type = self.packed_field_cls.UINT8
                        fields.append(field)
                        point_data_list.append(points[key].astype(np.uint8))
                        offset += 1

            # Add fields to pointcloud message
            for field in fields:
                pointcloud_msg.fields.append(field)

            # Set point stride
            point_stride = offset
            pointcloud_msg.point_stride = point_stride

            # Pack data
            packed_data = bytearray(num_points * point_stride)
            for i in range(num_points):
                byte_offset = i * point_stride
                for j, field_data in enumerate(point_data_list):
                    field = fields[j]
                    value = field_data[i]
                    if field.type == self.packed_field_cls.FLOAT32:
                        struct.pack_into(
                            "<f", packed_data, byte_offset + field.offset, float(value))
                    elif field.type == self.packed_field_cls.UINT32:
                        struct.pack_into(
                            "<I", packed_data, byte_offset + field.offset, int(value))
                    elif field.type == self.packed_field_cls.UINT8:
                        struct.pack_into(
                            "<B", packed_data, byte_offset + field.offset, int(value))

            pointcloud_msg.data = bytes(packed_data)

            # Yield the message
            yield self.output_topic_name, pointcloud_msg

        except Exception as e:
            logger.error(f"Error converting static point cloud data: {e}")
            return
