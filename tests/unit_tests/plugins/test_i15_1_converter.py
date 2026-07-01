from copy import deepcopy

from daq_queuing_service.plugins.i15_1_converter import (
    construct_blueapi_tasks_from_i15_1_experiment,
    construct_i15_1_blueapi_call_list,
)
from daq_queuing_service.task import ExperimentDefinition, Status, TaskWithPosition


def test_given_sample_name_in_correct_format_then_correct_sample_loaded():
    experiment_definition = ExperimentDefinition(
        plan_name="_",
        sample_id="test_sample",
        params={"sampleName": "test_8_1"},
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment_definition)
    assert tasks[0].name == "robot_load"
    assert tasks[0].params["position"] == "8"
    assert tasks[0].params["puck"] == "1"


def test_sample_centre_uses_expected_params():
    experiment_definition = ExperimentDefinition(
        plan_name="_",
        sample_id="test_sample",
        params={"sampleName": "test_8_1"},
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment_definition)
    assert tasks[1].name == "centre_sample"
    assert tasks[1].params == {
        "start_z": -20,
        "end_z": 0,
        "steps": 20,
        "exposure_time": 0.01,
    }


def test_session_and_number_of_tasks_per_experiment_is_expected():
    experiment_definition = ExperimentDefinition(
        plan_name="_",
        sample_id="test_sample",
        params={"sampleName": "test_8_1"},
        instrument_session="cm12345-1",
    )
    tasks = construct_blueapi_tasks_from_i15_1_experiment(experiment_definition)
    assert len(tasks) == 3
    for task in tasks:
        assert task.instrument_session == "cm12345-1"


def test_experiment_with_correct_plan_name_are_converted():
    experiment_definition = ExperimentDefinition(
        plan_name="run_full_collection",
        sample_id="test_sample",
        params={"sampleName": "test_8_1"},
        instrument_session="cm12345-1",
    )
    task = TaskWithPosition(
        experiment_definition=experiment_definition,
        id="1",
        status=Status.QUEUED,
        blueapi_calls=[],
        position=None,
    )
    call_list = construct_i15_1_blueapi_call_list([task], [], [])
    assert len(call_list) == 3


def test_mix_of_experiments_with_correct_plan_name_are_converted():
    good_experiment_definition = ExperimentDefinition(
        plan_name="run_full_collection",
        sample_id="test_sample",
        params={"sampleName": "test_8_1"},
        instrument_session="cm12345-1",
    )
    good_task = TaskWithPosition(
        experiment_definition=good_experiment_definition,
        id="1",
        status=Status.QUEUED,
        blueapi_calls=[],
        position=None,
    )

    bad_task = deepcopy(good_task)
    bad_task.experiment_definition.plan_name = "_"
    tasks = [good_task, bad_task, good_task]
    call_list = construct_i15_1_blueapi_call_list(tasks, [], [])
    assert len(call_list) == 6
