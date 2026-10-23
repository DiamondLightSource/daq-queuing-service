import logging

from colorlog import StreamHandler
from graypy import GELFTCPHandler
from pytest import LogCaptureFixture

from daq_queuing_service.app.config import GraylogConfig, StreamConfig
from daq_queuing_service.log import LOGGER, setup_logger


def test_setup_logger_sets_up_logger_correctly():
    logger = logging.Logger(name="test")
    setup_logger(
        logger,
        StreamConfig(level=logging.WARNING),
        GraylogConfig(enabled=True, port=1, url="test_port", level=logging.ERROR),
        propagate=False,
    )

    assert isinstance(logger.handlers[0], StreamHandler)
    assert logger.handlers[0].level == logging.WARNING
    assert isinstance(logger.handlers[1], GELFTCPHandler)
    assert logger.handlers[1].level == logging.ERROR
    assert logger.level == logging.WARNING  # min of handlers level
    assert not logger.propagate


def test_setup_logger_adds_beamline_to_all_logs(caplog: LogCaptureFixture):
    setup_logger(
        LOGGER,
        StreamConfig(level=logging.INFO),
        None,
        propagate=True,
        beamline="test beamline",
    )

    with caplog.at_level(logging.INFO):
        LOGGER.info("test log")

    assert caplog.records[0].beamline == "test beamline"  # type: ignore
