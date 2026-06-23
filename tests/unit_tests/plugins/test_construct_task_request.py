from blueapi.service.model import TaskRequest

from daq_queuing_service.plugins.construct_task_request import (
    construct_blueapi_task_request,
)
from daq_queuing_service.task import ExperimentDefinition


def test_construct_task_request_produces_expected_task_request_from_exp_definition():
    result = construct_blueapi_task_request(
        experiment_definition=ExperimentDefinition(
            plan_name="sleep",
            sample_id="test_sample",
            params={"time": 10},
            instrument_session="cm12345-1",
        )
    )
    assert result == TaskRequest(
        name="sleep", params={"time": 10}, instrument_session="cm12345-1"
    )
