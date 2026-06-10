"""
Central logging setup: console output only.
Call setup_logging() once at app entry (e.g. from tyro_cli).
"""
import logging
import sys
from pathlib import Path


def setup_logging(
    project_root: Path | None = None,
    log_level: int = logging.INFO,
    log_file_name: str | None = None,
) -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=date_fmt)

    root = logging.getLogger()
    root.setLevel(log_level)
    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Reduce noise from third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name (typically __name__)."""
    return logging.getLogger(name)
