from blueapi.service.model import TaskRequest

from daq_queuing_service.plugins.construct_task_request import (
    _construct_blueapi_task_request,
)
from daq_queuing_service.task import Experiment, ExperimentDefinition, Sample


def test_construct_task_request_produces_expected_task_request_from_exp_definition():
    result = _construct_blueapi_task_request(
        experiment=Experiment(
            name="test_experiment",
            instrument_session="cm12345-1",
            experiment_definition=ExperimentDefinition(
                name="sleep",
                id="",
                data={"time": 10},
            ),
            sample=Sample(name="test_sample", id="test_sample", data={}),
        )
    )
    assert result == TaskRequest(
        name="sleep", params={"time": 10}, instrument_session="cm12345-1"
    )
