import os
import importlib
import sys
from pathlib import Path
from base64 import standard_b64encode
from functools import lru_cache

sys.path.insert(0, str(Path(__file__).parent / "py"))


def get_proto_descriptor_bin(proto_cls):
    import google.protobuf.descriptor_pb2
    file_descriptor_set = google.protobuf.descriptor_pb2.FileDescriptorSet()
    added_files = set()

    def build_fds(proto_file_descriptor):
        if proto_file_descriptor.name in added_files:
            return
        for depencency in proto_file_descriptor.dependencies:
            build_fds(depencency)
        file_descriptor_set.file.add().ParseFromString(proto_file_descriptor.serialized_pb)
        added_files.add(proto_file_descriptor.name)

    build_fds(proto_cls.DESCRIPTOR.file)
    return file_descriptor_set.SerializeToString()


def get_proto_descriptor_base64(proto_cls):
    return standard_b64encode(get_proto_descriptor_bin(proto_cls)).decode("ascii")


@lru_cache(maxsize=None)
def get_pb2(type_ident: str):
    """Returns the corresponding protobuf message class based on `type_ident`. Supported formats:
    - `builtins/Time`, `builtins/Duration` → `lightwheel.builtins_pb2`
    - `package.MessageName` (with dots) → `py.package.MessageName_pb2`
    """

    if type_ident.startswith("builtins/"): # Use the builtins subproject package
        name = type_ident.split("/")[-1]
        mod = importlib.import_module(f"{__name__}.py.lightwheel.builtins_pb2")
        return getattr(mod, name)

    # Dotted identifier (e.g., foxglove.PointCloud, lightwheel.SubtaskAnnotation)
    if "." in type_ident:
        parts = type_ident.split(".")
        module_name = f"{__name__}.py.{type_ident}_pb2"
        return getattr(importlib.import_module(module_name), parts[-1])

    raise ValueError(f"unknown type_ident: {type_ident}")
