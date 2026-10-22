import logging

import colorlog
from graypy import GELFTCPHandler

from daq_queuing_service.app.config import GraylogConfig, StreamConfig

LOGGER = logging.getLogger("Queue")


class BeamlineFilter(logging.Filter):
    def __init__(self, beamline: str):
        self.beamline = beamline

    def filter(self, record: logging.LogRecord):
        record.beamline = self.beamline
        return True


def setup_logger(
    logger: logging.Logger,
    stream: StreamConfig | None,
    graylog: GraylogConfig | None,
    propagate: bool,
    beamline: str | None,
):
    logger.handlers.clear()
    logger.filters.clear()

    if stream:
        handler = colorlog.StreamHandler()
        handler.setFormatter(
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
        logger.addHandler(handler)
        handler.setLevel(stream.level)

    if graylog and graylog.enabled:
        graylog_handler = GELFTCPHandler(host=graylog.url, port=graylog.port)
        graylog_handler.setLevel(graylog.level)
        logger.addHandler(graylog_handler)

    logger.propagate = propagate

    if beamline:
        logger.addFilter(BeamlineFilter(beamline))
