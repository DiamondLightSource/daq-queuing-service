import logging

import colorlog

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
LOGGER = logging.getLogger("Queue")
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)
LOGGER.propagate = False
