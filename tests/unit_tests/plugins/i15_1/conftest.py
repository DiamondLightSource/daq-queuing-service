from unittest.mock import MagicMock, patch

import pytest
from daq_config_server.models.i15_1 import StandardsPin, StandardsPuck

from daq_queuing_service.plugins.i15_1.tiled_interaction import cache


@pytest.fixture(autouse=True)
def clear_cache():
    yield
    cache.clear()


@pytest.fixture
def standards_puck():
    return StandardsPuck(
        pins={
            1: StandardsPin(capillary="metal", contents=None),
            2: StandardsPin(capillary="bs1.0", contents="Silicon"),
            3: StandardsPin(capillary="fq1.0", contents="Silicon"),
            4: StandardsPin(capillary="bs1.5", contents="Silicon"),
            5: None,
            6: StandardsPin(capillary="bs2.0", contents="Silicon"),
            7: None,
            8: None,
            9: StandardsPin(capillary="bs1.0", contents="Si/Al2O3"),
            10: StandardsPin(capillary="fq1.0", contents="Si/Al2O3"),
            11: StandardsPin(capillary="fq1.5", contents="Si/Al2O3"),
            12: StandardsPin(capillary="fq2.0", contents="Si/Al2O3"),
            13: StandardsPin(capillary="bs1.0", contents="Pb"),
            14: StandardsPin(capillary="bs1.0", contents="LaB6 660b"),
            15: StandardsPin(capillary="bs1.0", contents=None),
            16: StandardsPin(capillary="fq1.0", contents=None),
            17: StandardsPin(capillary="bs1.5", contents=None),
            18: StandardsPin(capillary="fq1.5", contents=None),
            19: StandardsPin(capillary="bs2.0", contents=None),
            20: StandardsPin(capillary="fq2.0", contents=None),
            21: StandardsPin(capillary="bs1.0", contents="Ga/In"),
            22: StandardsPin(capillary="bs1.0", contents="Tungsten/Boron mix"),
        }
    )


@pytest.fixture(autouse=True)
def mock_config_client(standards_puck: StandardsPuck):
    mock_config_server = MagicMock()
    mock_config_server.get_file_contents = MagicMock(return_value=standards_puck)
    with patch(
        "daq_config_server.client.ConfigClient.from_url",
        return_value=mock_config_server,
    ):
        yield mock_config_server
