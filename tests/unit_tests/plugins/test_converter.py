import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.converter import (
    Converter,
    get_converter,
)
from daq_queuing_service.task_queue.task import (
    Experiment,
    ExperimentDefinition,
    Sample,
    Task,
    TaskWithPosition,
)


def test_get_converter_returns_converter_from_path_and_name():
    converter = get_converter("daq_queuing_service.plugins.converter", "Converter")
    assert isinstance(converter, Converter)


def test_get_converter_raises_error_if_imported_class_is_not_converter_type():
    with pytest.raises(TypeError):
        get_converter("daq_queuing_service.broadcaster", "Broadcaster")


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


def test_default_converter_pre_process_does_not_modify_queue(tasks: list[Task]):
    new_queue = Converter().pre_process(tasks, [], [])
    assert new_queue == tasks


def test_default_converter_construct_blueapi_calls_creates_one_blueapi_call_per_task(
    bluesky_tasks: list[Task],
):
    task_copies = [TaskWithPosition.from_task(task) for task in bluesky_tasks]
    blueapi_calls = Converter().construct_blueapi_calls(task_copies, [], [])
    for task, blueapi_call in zip(task_copies, blueapi_calls, strict=True):
        assert isinstance(task.experiment, TaskRequest)
        assert blueapi_call == BlueapiCall(
            task_request=task.experiment, parent_task_id=task.id
        )
