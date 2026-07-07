from copy import deepcopy

from blueapi.service.model import TaskRequest

from daq_queuing_service.plugins.i15_1_converter import (
    construct_blueapi_tasks_from_i15_1_experiment,
    construct_i15_1_blueapi_call_list,
)
from daq_queuing_service.task import (
    Experiment,
    ExperimentDefinition,
    Sample,
    Status,
    TaskKind,
    TaskWithPosition,
)


def test_given_sample_name_in_correct_format_then_correct_sample_loaded():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(name=" ", id="", data={}),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment)
    assert tasks[0].name == "robot_load"
    assert tasks[0].params["position"] == "8"
    assert tasks[0].params["puck"] == "1"


def test_sample_centre_uses_expected_params():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(name=" ", id="", data={}),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment)
    assert tasks[1].name == "centre_sample"
    assert tasks[1].params == {
        "start_z": -20,
        "end_z": 0,
        "steps": 20,
        "exposure_time": 0.01,
        "metadata": {
            "experiment_definition": ExperimentDefinition(name=" ", id="", data={}),
            "sample": Sample(name="test_8_1", id="", data={}),
        },
    }


def test_session_and_number_of_tasks_per_experiment_is_expected():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(name=" ", id="", data={}),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment)
    assert len(tasks) == 3
    for task in tasks:
        assert task.instrument_session == "cm12345-1"


def test_experiment_with_correct_experiment_type_are_converted():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    task = TaskWithPosition(
        experiment=experiment,
        id="1",
        status=Status.QUEUED,
        blueapi_calls=[],
        position=None,
        kind=TaskKind.EXPERIMENT,
    )
    call_list = construct_i15_1_blueapi_call_list([task], [], [])
    assert len(call_list) == 3


def test_mix_of_experiments_with_correct_experiment_type_are_converted():
    good_experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    good_task = TaskWithPosition(
        experiment=good_experiment,
        id="1",
        status=Status.QUEUED,
        blueapi_calls=[],
        position=None,
        kind=TaskKind.EXPERIMENT,
    )

    class BadExperiment: ...

    bad_task = deepcopy(good_task)
    bad_task.experiment = BadExperiment()  # type: ignore
    plan_task = deepcopy(good_task)
    plan_task.experiment = TaskRequest(name="", instrument_session="")
    plan_task.kind = TaskKind.PLAN
    tasks = [good_task, bad_task, plan_task, good_task]
    call_list = construct_i15_1_blueapi_call_list(tasks, [], [])
    assert len(call_list) == 7
