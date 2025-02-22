# flake8: noqa: F401
import logging
from pathlib import Path

from . import commands, config, tpch
from ._version import __version__

app_root = config.Config.app_root


def setup_logger(level=logging.WARNING):
    log_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    )
    stream_formatter = logging.Formatter("[%(levelname)s] %(message)s")

    logger = logging.getLogger()
    logger.setLevel(level)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(stream_formatter)

    file_handler = logging.FileHandler(
        Path(app_root).expanduser().joinpath("tpch_runner.log")
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger(logging.INFO)
