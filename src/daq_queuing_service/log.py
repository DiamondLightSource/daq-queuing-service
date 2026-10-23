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
    stream: StreamConfig,
    graylog: GraylogConfig | None = None,
    propagate: bool = False,
    beamline: str | None = None,
):
    """Setup a logger for stream and optionally graylog

    Args:
        logger (logging.Logger): Logger to set up
        stream (StreamConfig): Configuration for stream handler
        graylog (GraylogConfig | None): Configuration for graylog handler.
            Defaults to None
        propagate (bool): Whether log records should be forwarded to parent loggers
            in the logging hierarchy after being processed by this logger.
            Defaults to False.
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

    logger.setLevel(min(handler.level for handler in logger.handlers))
    logger.debug(f"Set up logger: {logger} with handlers: {logger.handlers}")
