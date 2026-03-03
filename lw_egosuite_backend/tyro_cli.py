from typing import Union
from typing_extensions import Annotated
import sys
import tyro

from lw_egosuite_backend.logging_config import setup_logging
from lw_egosuite_backend.std_pipeline import StdPipeline
from lw_egosuite_backend.server import Show

Commands = Union[
    Annotated[StdPipeline, tyro.conf.subcommand(name="convert")],
    Annotated[Show, tyro.conf.subcommand(name="show")],
]


def entrypoint():
    setup_logging()  # console + project_dir/output/logs/
    tyro.extras.set_accent_color("magenta")
    sys.exit(tyro.cli(Commands).run())


if __name__ == "__main__":
    entrypoint()
