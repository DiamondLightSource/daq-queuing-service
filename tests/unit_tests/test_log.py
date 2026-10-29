import logging

import pytest
from colorlog import StreamHandler
from graypy import GELFTCPHandler
from pytest import LogCaptureFixture

from daq_queuing_service.app.config import GraylogConfig, LoggingHandlerConfig
from daq_queuing_service.log import (
    LOGGER,
    add_session_to_logger,
    get_session_filter,
    setup_logger,
)


def test_setup_logger_sets_up_logger_correctly():
    logger = logging.Logger(name="test")
    setup_logger(
        logger,
        LoggingHandlerConfig(level=logging.WARNING),
        GraylogConfig(enabled=True, port=1, url="test_port", level=logging.ERROR),
        propagate=False,
    )

    assert isinstance(logger.handlers[0], StreamHandler)
    assert logger.handlers[0].level == logging.WARNING
    assert isinstance(logger.handlers[1], GELFTCPHandler)
    assert logger.handlers[1].level == logging.ERROR
    assert logger.level == logging.WARNING  # min of handlers level
    assert not logger.propagate


def test_setup_logger_adds_instrument_to_all_logs(caplog: LogCaptureFixture):
    setup_logger(
        LOGGER,
        LoggingHandlerConfig(level=logging.INFO),
        None,
        propagate=True,
        instrument="test instrument",
    )

    with caplog.at_level(logging.INFO):
        LOGGER.info("test log")

    assert caplog.records[0].instrument == "test instrument"  # type: ignore


def test_add_session_to_logger_adds_session_to_logs_and_leaving_removes_it(
    caplog: LogCaptureFixture,
):
    with caplog.at_level(logging.INFO):
        with add_session_to_logger("cm12345-1", LOGGER):
            LOGGER.info("test log")
        LOGGER.info("test log 2")

    assert caplog.records[0].instrument_session == "cm12345-1"  # type: ignore
    assert getattr(caplog.records[1], "instrument_session", None) is None


def test_add_session_to_logger_raises_if_different_session_already_added():
    with add_session_to_logger("cm12345-1", LOGGER):
        with pytest.raises(ValueError):
            with add_session_to_logger("cm54321-1", LOGGER):
                ...


def test_add_session_to_logger_nested_with_same_session_adds_session_in_every_layer(
    caplog: LogCaptureFixture,
):
    with add_session_to_logger("cm12345-1", LOGGER):
        LOGGER.info("test log")
        with add_session_to_logger("cm12345-1", LOGGER):
            LOGGER.info("test log 2")
        LOGGER.info("test log 3")
    LOGGER.info("test log 4")

    for i in (0, 1, 2):
        assert caplog.records[i].instrument_session == "cm12345-1"  # type: ignore
    assert getattr(caplog.records[3], "instrument_session", None) is None


def test_get_session_filter_raises_error_if_logger_does_not_have_one():
    with pytest.raises(RuntimeError):
        get_session_filter(logging.Logger(""))
