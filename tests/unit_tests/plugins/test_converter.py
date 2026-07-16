from blueapi.service.model import TaskRequest

from daq_queuing_service.plugins.converter import (
    Converter,
    get_converter,
)
from daq_queuing_service.task import Experiment, ExperimentDefinition, Sample


def test_get_converter_returns_converter_from_path_and_name():
    converter = get_converter(
        "daq_queuing_service.plugins.converter",
        "Converter",
    )
    assert isinstance(converter, Converter)


def test_default_converter_produces_expected_task_request_from_exp_definition():
    result = Converter()._construct_blueapi_task_request(
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
