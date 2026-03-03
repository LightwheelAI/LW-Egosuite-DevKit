"""
Central logging setup: console + file under project_dir/output/logs/.
Call setup_logging() once at app entry (e.g. from tyro_cli).
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(
    project_root: Path | None = None,
    log_level: int = logging.INFO,
    log_file_name: str | None = None,
) -> None:
    """
    Configure root logger to output to console and to project_root/output/logs/.
    Creates output/logs directory if it does not exist.
    """
    if project_root is None:
        project_root = Path.cwd()
    log_dir = project_root / "output" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if log_file_name is None:
        # One log file per run: app_YYYY-MM-DD_HH-MM-SS.log
        log_file_name = f"app_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_file = log_dir / log_file_name

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

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Reduce noise from third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name (typically __name__)."""
    return logging.getLogger(name)
