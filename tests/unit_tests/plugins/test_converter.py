import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.external_interaction.blueapi.blueapi_call import BlueapiCall
from daq_queuing_service.plugins import (
    QueuePlugin,
    get_queue_plugin,
)
from daq_queuing_service.task_queue.task import (
    Experiment,
    ExperimentDefinition,
    Task,
    TaskWithPosition,
)

from ..conftest import make_sample


def test_get_queue_plugin_returns_plugin_from_path_and_name():
    plugin = get_queue_plugin("daq_queuing_service.plugins", "QueuePlugin")
    assert isinstance(plugin, QueuePlugin)


def test_get_queue_plugin_raises_error_if_imported_class_is_not_queue_plugin_type():
    with pytest.raises(TypeError):
        get_queue_plugin("daq_queuing_service.broadcaster", "Broadcaster")


def test_default_plugin_raises_error_when_converting_ulims_experiment():
    with pytest.raises(NotImplementedError):
        QueuePlugin()._construct_blueapi_task_request(
            experiment=Experiment(
                name="test_experiment",
                instrument_session="cm12345-1",
                experiment_definition=ExperimentDefinition(
                    name="sleep",
                    id="",
                    data={"time": 10},
                ),
                sample=make_sample("test_sample", "test_sample"),
            )
        )


def test_default_plugin_pre_process_does_not_modify_queue(tasks: list[Task]):
    new_queue = QueuePlugin().pre_process(None, tasks, [], [])
    assert new_queue == tasks


def test_default_plugin_construct_blueapi_calls_creates_one_blueapi_call_per_task(
    bluesky_tasks: list[Task],
):
    task_copies = [TaskWithPosition.from_task(task) for task in bluesky_tasks]
    blueapi_calls = QueuePlugin().construct_blueapi_calls(task_copies, [], [])
    for task, blueapi_call in zip(task_copies, blueapi_calls, strict=True):
        assert isinstance(task.experiment, TaskRequest)
        assert blueapi_call == BlueapiCall(
            task_request=task.experiment, parent_task_id=task.id
        )
