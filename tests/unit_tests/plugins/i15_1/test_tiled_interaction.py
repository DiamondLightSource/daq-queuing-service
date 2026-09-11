import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr
from pytest import LogCaptureFixture
from tiled.queries import Comparison, Eq, KeyPresent

from daq_queuing_service.plugins.i15_1.backgrounds import (
    BackgroundInfo,
    TiledBackground,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    TILED_STALE_TIME,
    TILED_URL,
    get_suitable_tiled_background,
    get_tiled_client,
)


@pytest.fixture(autouse=True)
def mock_current_time():
    with patch(
        "daq_queuing_service.plugins.i15_1.tiled_interaction.time.time", return_value=30
    ) as mock_current_time:
        yield mock_current_time


@pytest.fixture()
def mock_tiled_searches(
    tiled_client: MagicMock,
) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock, MagicMock, MagicMock, MagicMock]:
    result_1 = MagicMock()

    result_1.metadata = {
        "start": {
            "time": 1,
            "experiment_definition": {"data": {"time_per_pdf": 10}},
            "sample_info": {"data": {"capillary": "air"}},
            "data_session_directory": "/path/to/data/2026/cm12345-1",
            "scan_file": "i15-1-10000",
        }
    }
    result_2 = MagicMock()
    result_2.metadata = {
        "start": {
            "time": 10,
            "experiment_definition": {"data": {"time_per_pdf": 11}},
            "sample_info": {"data": {"capillary": "fq1.0"}},
            "data_session_directory": "/path/to/data/2026/cm12345-1",
            "scan_file": "i15-1-10001",
        }
    }
    result_3 = MagicMock()
    result_3.metadata = {
        "start": {
            "time": 2,
            "experiment_definition": {"data": {"time_per_pdf": 9}},
            "sample_info": {"data": {"capillary": "fq1.0"}},
            "data_session_directory": "/path/to/data/2026/cm12345-1",
            "scan_file": "i15-1-10002",
        }
    }
    result_4 = MagicMock()
    result_4.metadata = {
        "start": {
            "time": 5,
            "experiment_definition": {"data": {"time_per_pdf": 12}},
            "sample_info": {"data": {"capillary": "fq1.0"}},
            "data_session_directory": "/path/to/data/2026/cm12345-1",
            "scan_file": "i15-1-10003",
        }
    }

    search_result_7 = MagicMock()
    search_result_7.search = MagicMock(
        return_value={
            "tiled_id_1": result_1,
            "tiled_id_2": result_2,
            "tiled_id_3": result_3,
            "tiled_id_4": result_4,
        }
    )

    search_result_6 = MagicMock()
    search_result_6.search = MagicMock(return_value=search_result_7)
    search_result_5 = MagicMock()
    search_result_5.search = MagicMock(return_value=search_result_6)
    search_result_4 = MagicMock()
    search_result_4.search = MagicMock(return_value=search_result_5)
    search_result_3 = MagicMock()
    search_result_3.search = MagicMock(return_value=search_result_4)
    search_result_2 = MagicMock()
    search_result_2.search = MagicMock(return_value=search_result_3)

    tiled_client.search = MagicMock(return_value=search_result_2)

    return (
        tiled_client,
        search_result_2,
        search_result_3,
        search_result_4,
        search_result_5,
        search_result_6,
        search_result_7,
    )


def test_get_suitable_tiled_background_makes_expected_searches(
    mock_tiled_searches: tuple[
        MagicMock, MagicMock, MagicMock, MagicMock, MagicMock, MagicMock, MagicMock
    ],
):
    client, search_2, search_3, search_4, search_5, search_6, search_7 = (
        mock_tiled_searches
    )
    get_suitable_tiled_background(
        client,
        BackgroundInfo(instrument_session="cm12345-1", bg_type="air", time_per_pdf=10),
    )
    client.search.assert_called_once_with(Eq(key="start.instrument", value="i15-1"))
    search_2.search.assert_called_once_with(
        Eq(key="start.instrument_session", value="cm12345-1")
    )
    search_3.search.assert_called_once_with(Eq("stop.exit_status", "success"))
    search_4.search.assert_called_once_with(
        Comparison("ge", "stop.time", 30 - TILED_STALE_TIME)
    )
    search_5.search.assert_called_once_with(Eq("start.background", True))
    search_6.search.assert_called_once_with(
        KeyPresent("start.sample_info.data.capillary")
    )
    search_7.search.assert_called_once_with(
        KeyPresent("start.experiment_definition.data.time_per_pdf")
    )


def test_get_background_tiled_returns_most_recent_valid_background(
    mock_tiled_searches: tuple[MagicMock, ...],
):
    client, *_ = mock_tiled_searches
    result = get_suitable_tiled_background(
        client,
        BackgroundInfo(
            instrument_session="cm12345-1", bg_type="fq1.0", time_per_pdf=10
        ),
    )
    assert result == TiledBackground(
        instrument_session="cm12345-1",
        tiled_id="tiled_id_2",
        bg_type="fq1.0",
        time_per_pdf=11,
        filepath=Path("/path/to/data/2026/cm12345-1/i15-1-10001.nxs"),
        filename=Path("i15-1-10001.nxs"),
    )


def test_get_suitable_tiled_background_returns_none_if_no_matching_backgrounds_found(
    mock_tiled_searches: tuple[MagicMock, ...],
):
    client, *_, final_search = mock_tiled_searches
    final_search.search.return_value = {}
    assert (
        get_suitable_tiled_background(
            client,
            BackgroundInfo(
                instrument_session="cm12345-1", bg_type="air", time_per_pdf=10
            ),
        )
        is None
    )


def test_get_tiled_client_instantiates_client_from_uri():
    with patch(
        "daq_queuing_service.plugins.i15_1.tiled_interaction.from_uri"
    ) as mock_from_uri:
        tiled_client = get_tiled_client()

    mock_from_uri.assert_called_once_with(TILED_URL, auth=None)
    assert tiled_client is mock_from_uri.return_value


@patch("daq_queuing_service.plugins.i15_1.tiled_interaction.from_uri")
def test_get_tiled_client_warns_if_no_client_id_or_secret_found(
    mock_from_uri: MagicMock, caplog: LogCaptureFixture
):
    with caplog.at_level(logging.WARNING):
        get_tiled_client()

    assert "No UDC client ID found." in caplog.text
    assert "No UDC secret found." in caplog.text
    assert "Tiled auth will not be used."


@patch("daq_queuing_service.plugins.i15_1.tiled_interaction.TiledAuth")
@patch("daq_queuing_service.plugins.i15_1.tiled_interaction.ServiceAccount")
@patch("daq_queuing_service.plugins.i15_1.tiled_interaction.from_uri")
@patch("daq_queuing_service.plugins.i15_1.tiled_interaction.os.environ.get")
def test_get_tiled_client_adds_tiled_auth_with_client_id_and_secret_if_found(
    mock_env_var_get: MagicMock,
    mock_from_uri: MagicMock,
    mock_service_account_cls: MagicMock,
    mock_tiled_auth_cls: MagicMock,
):
    values = iter(["client_id", "secret"])

    def get_next_env_variable(*_, **__):  # type:ignore
        return next(values)

    mock_env_var_get.side_effect = get_next_env_variable

    get_tiled_client()

    mock_service_account_cls.assert_called_once_with(
        client_id="client_id",
        client_secret=SecretStr("secret"),
        token_url="https://identity.diamond.ac.uk/realms/dls/protocol/openid-connect/token",
    )
    mock_tiled_auth_cls.assert_called_once_with(
        tiled_auth=mock_service_account_cls.return_value
    )
    mock_from_uri.assert_called_once_with(
        TILED_URL,
        auth=mock_tiled_auth_cls.return_value,
    )
