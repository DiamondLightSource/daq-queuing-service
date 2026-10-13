from typing import Any
from unittest.mock import patch

import pytest
from daq_config_server.client import ConfigClient
from daq_config_server.models.i15_1 import (
    CentringParameters,
    DataCollectionParameters,
    QueueParameters,
    StandardsPin,
    StandardsPuck,
)
from daq_config_server.testing import MockServerResponse

from daq_queuing_service.plugins.i15_1.i15_1_converter import (
    QUEUE_PARAMS_CONFIG_PATH,
    STANDARDS_PUCK_CONFIG_PATH,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import cache


@pytest.fixture(autouse=True)
def clear_cache():
    yield
    cache.clear()


@pytest.fixture
def standards_puck() -> StandardsPuck:
    return StandardsPuck(
        position_on_table=1,
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
        },
    )


@pytest.fixture
def queue_params() -> QueueParameters:
    return QueueParameters(
        do_wait_for_beam=True,
        do_centring=True,
        do_robot_load=True,
        add_auxiliary_scans=True,
        tiled_stale_time_s=604800,
        centring_params=CentringParameters(
            start_z=-20, end_z=7, step_size_mm=0.1, exposure_time_per_frame_s=0.1
        ),
        data_collection_params=DataCollectionParameters(exposure_time_per_frame_s=1),
    )


@pytest.fixture
def mock_config(
    standards_puck: StandardsPuck, queue_params: QueueParameters
) -> dict[str, Any]:
    return {
        STANDARDS_PUCK_CONFIG_PATH: standards_puck,
        QUEUE_PARAMS_CONFIG_PATH: queue_params,
    }


@pytest.fixture(autouse=True)
def mock_config_client(mock_config: dict[str, Any]):
    config_client = ConfigClient(server_response=MockServerResponse(mock_config))
    with patch(
        "daq_config_server.client.ConfigClient.from_url",
        return_value=config_client,
    ):
        yield config_client
