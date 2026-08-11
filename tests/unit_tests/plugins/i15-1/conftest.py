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
        "daq_queuing_service.plugins.i15_1.i15_1_converter.from_uri"
    ) as mock_from_uri:
        yield mock_from_uri.return_value
