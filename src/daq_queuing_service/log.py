import logging

import colorlog
from graypy import GELFTCPHandler

from daq_queuing_service.app.config import GraylogConfig, StreamConfig

LOGGER = logging.getLogger("Queue")
LOGGER.setLevel(logging.DEBUG)


class BeamlineFilter(logging.Filter):
    def __init__(self, beamline: str):
        self.beamline = beamline

    def filter(self, record: logging.LogRecord):
        record.beamline = self.beamline
        return True


def setup_logger(
    logger: logging.Logger,
    stream: StreamConfig,
    graylog: GraylogConfig | None,
    propagate: bool = False,
    beamline: str | None = None,
):
    """_summary_

    Args:
        logger (logging.Logger): Logger to set up
        stream (StreamConfig): Configuration for stream handler
        graylog (GraylogConfig | None): Configuration for graylog handler
        propagate (bool): Whether log records should be passed from handlers to parent
            loggers. Defaults to False.
        beamline (str, optional): Optionally add a beamline to the logger, to be added
            as context to all logs. Defaults to None
    """
    logger.handlers.clear()
    logger.filters.clear()

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

    logger.info(f"Set up logger: {logger} with handlers: {logger.handlers}")
