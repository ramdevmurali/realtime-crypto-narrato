import logging
import sys


def _configure_root_once():
    if logging.getLogger().handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def get_logger(name: str) -> logging.Logger:
    _configure_root_once()
    return logging.getLogger(name)
