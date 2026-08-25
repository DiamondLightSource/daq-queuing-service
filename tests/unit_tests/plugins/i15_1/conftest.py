from unittest.mock import patch

import pytest

from daq_queuing_service.plugins.i15_1.tiled_interaction import cache


@pytest.fixture(autouse=True)
def clear_cache():
    yield
    cache.clear()


@pytest.fixture(autouse=True)
def tiled_client():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_tiled_client"
    ) as mock_get_tiled_client:
        yield mock_get_tiled_client.return_value
