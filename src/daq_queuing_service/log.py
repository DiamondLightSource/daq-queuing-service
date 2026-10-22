import logging

import colorlog
from graypy import GELFTCPHandler

HANDLER = colorlog.StreamHandler()
HANDLER.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
)
GRAYLOG_HANDLER = GELFTCPHandler(
    "graylog-log-target.diamond.ac.uk",
    # https://gitlab.diamond.ac.uk/sscc-docs/developer-guide/-/merge_requests/295/diffs
    12231,  # dodal port - should make dedicated one for the queue
)

LOGGER = logging.getLogger("Queue")


def setup_logger(
    log_level: int = logging.INFO, graylog: bool = True, propagate: bool = False
):
    LOGGER.handlers.clear()
    LOGGER.addHandler(HANDLER)
    if graylog:
        LOGGER.addHandler(GRAYLOG_HANDLER)

    LOGGER.setLevel(log_level)
    for handler in LOGGER.handlers:
        handler.setLevel(log_level)

    LOGGER.propagate = propagate
