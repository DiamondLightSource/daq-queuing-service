from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from tiled.queries import Eq

from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo
from daq_queuing_service.plugins.i15_1.tiled_interaction import get_background_tiled_id


@pytest.fixture()
def mock_tiled_searches() -> Generator[
    tuple[MagicMock, MagicMock, MagicMock], Any, None
]:
    result_1 = MagicMock()
    result_1.metadata = {"start": {"time": 1}}
    result_2 = MagicMock()
    result_2.metadata = {"start": {"time": 10}}
    result_3 = MagicMock()
    result_3.metadata = {"start": {"time": 2}}

    search_result_3 = MagicMock()
    search_result_3.search = MagicMock(
        return_value={
            "tiled_id_1": result_1,
            "tiled_id_2": result_2,
            "tiled_id_3": result_3,
        }
    )

    search_result_2 = MagicMock()
    search_result_2.search = MagicMock(return_value=search_result_3)

    client = MagicMock()
    client.search = MagicMock(return_value=search_result_2)

    with patch(
        "daq_queuing_service.plugins.i15_1.tiled_interaction.from_uri",
        MagicMock(return_value=client),
    ):
        yield client, search_result_2, search_result_3


def test_get_background_tiled_id_makes_expected_searches(
    mock_tiled_searches: tuple[MagicMock, MagicMock, MagicMock],
):
    client, search_2, search_3 = mock_tiled_searches
    get_background_tiled_id(
        BackgroundInfo(bg_type="air", cobra=False, blower=False),
        instrument_session="cm12345-1",
    )
    client.search.assert_called_once_with(
        Eq(key="start.instrument_session", value="cm12345-1")
    )
    search_2.search.assert_called_once_with(Eq(key="start.instrument", value="i15-1"))
    search_3.search.assert_called_once_with(
        Eq(
            key="start.experiment_definition.metadata.background",
            value='{"bg_type":"air","cobra":false,"blower":false}',
        )
    )


def test_get_background_tiled_returns_most_recent_valid_background(
    mock_tiled_searches: tuple[MagicMock, MagicMock, MagicMock],
):
    assert (
        get_background_tiled_id(
            BackgroundInfo(bg_type="air", cobra=False, blower=False),
            instrument_session="cm12345-1",
        )
        == "tiled_id_2"
    )


def test_get_background_tiled_id_returns_none_if_no_matching_backgrounds_found(
    mock_tiled_searches: tuple[MagicMock, MagicMock, MagicMock],
):
    _, _, final_search = mock_tiled_searches
    final_search.search.return_value = {}
    assert (
        get_background_tiled_id(
            BackgroundInfo(bg_type="air", cobra=False, blower=False),
            instrument_session="cm12345-1",
        )
        is None
    )
