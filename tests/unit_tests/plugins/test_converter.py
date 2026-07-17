import pytest

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


def test_default_converter_raises_error_when_converting_ulims_experiment():
    with pytest.raises(NotImplementedError):
        Converter()._construct_blueapi_task_request(
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
