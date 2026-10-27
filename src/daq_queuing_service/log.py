from contextlib import contextmanager
from contextvars import ContextVar
from logging import Filter, Logger, LogRecord, getLogger
from typing import Any

import colorlog
from graypy import GELFTCPHandler

from daq_queuing_service.app.config import (
    GraylogConfig,
    LoggingHandlerConfig,
    LogLevel,
)

LOGGER = getLogger("Queue")


class InstrumentFilter(Filter):
    def __init__(self, instrument: str):
        self.instrument = instrument

    def filter(self, record: LogRecord):
        record.instrument = self.instrument
        return True


class InstrumentSessionFilter(Filter):
    def __init__(self, instrument_session_var: ContextVar[str | None]):
        self.instrument_session_var = instrument_session_var

    def filter(self, record: LogRecord):
        if session := self.instrument_session_var.get():
            record.instrument_session = session
        return True


def setup_logger(
    logger: Logger,
    stream: LoggingHandlerConfig,
    graylog: GraylogConfig | None = None,
    propagate: bool = False,
    instrument: str | None = None,
):
    """Setup a logger for stream and optionally graylog

    Args:
        logger (logging.Logger): Logger to set up
        stream (LoggingHandlerConfig): Configuration for stream handler
        graylog (GraylogConfig | None): Configuration for graylog handler.
            Defaults to None
        propagate (bool): Whether log records should be forwarded to parent loggers
            in the logging hierarchy after being processed by this logger.
            Defaults to False.
        instrument (str, optional): Optionally add a instrument to the logger, to be
        added as context to all logs. Defaults to None
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

    if instrument:
        logger.addFilter(InstrumentFilter(instrument))
    logger.addFilter(
        InstrumentSessionFilter(ContextVar("instrument_session", default=None))
    )

    logger.setLevel(min(handler.level for handler in logger.handlers))
    logger.debug(f"Set up logger: {logger} with handlers: {logger.handlers}")


def get_session_filter(logger: Logger) -> InstrumentSessionFilter:
    for f in logger.filters:
        if isinstance(f, InstrumentSessionFilter):
            return f

    raise RuntimeError("Logger has no InstrumentSessionFilter attached.")


@contextmanager
def add_session_to_logger(instrument_session: str, logger: Logger = LOGGER):
    session_filter = get_session_filter(logger)

    current = session_filter.instrument_session_var.get()

    if current is not None and current != instrument_session:
        raise ValueError(
            f"Cannot add session {instrument_session} to logger, session {current} "
            + "already added"
        )

    token = session_filter.instrument_session_var.set(instrument_session)

    try:
        yield
    finally:
        session_filter.instrument_session_var.reset(token)


def notify(
    message: str,
    level: LogLevel = "INFO",
    logger: Logger = LOGGER,
    **kwargs: Any,
):
    kwargs["extra"] = {
        **kwargs.get("extra", {}),
        "event_type": "notification",
    }

    getattr(logger, level.lower())(message, **kwargs)
