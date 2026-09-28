import pytest

from daq_queuing_service.plugins.i15_1.tiled_interaction import cache


@pytest.fixture(autouse=True)
def clear_cache():
    yield
    cache.clear()
