from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo
from daq_queuing_service.plugins.i15_1.i15_1_converter import I151Converter
from daq_queuing_service.task_queue.task import (
    Experiment,
    ExperimentDefinition,
    Sample,
    Status,
    Task,
    TaskKind,
    TaskWithPosition,
)


def assert_tasks_equal(task1: Task | TaskWithPosition, task2: Task | TaskWithPosition):
    # Check two tasks are equal other than the generated UUID
    copy1 = type(task1).model_validate(task1)
    copy2 = type(task2).model_validate(task2)
    copy1.id = copy2.id = ""
    assert task1 == task2


@pytest.fixture(autouse=True)
def background_found_in_tiled():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_background_tiled_id",
        MagicMock(return_value="fake_tiled_id"),
    ) as mock_get_background_tiled_id:
        yield mock_get_background_tiled_id


@pytest.fixture()
def background_not_found_in_tiled():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_background_tiled_id",
        MagicMock(return_value=None),
    ) as mock_get_background_tiled_id:
        yield mock_get_background_tiled_id


@pytest.fixture()
def tasks_and_calls(
    tasks: list[Task],
) -> tuple[list[TaskWithPosition], list[BlueapiCall]]:
    tasks_with_positions = [TaskWithPosition.from_task(task) for task in tasks]
    calls: list[BlueapiCall] = []
    for task in tasks_with_positions:
        assert isinstance(task.experiment, Experiment)
        calls.extend(
            [
                BlueapiCall(
                    task_request=task_request,
                    parent_task_id=task.id,
                )
                for task_request in (
                    I151Converter()._construct_blueapi_tasks_from_experiment(
                        task.experiment
                    )
                )
            ]
        )
    return tasks_with_positions, calls


def test_given_sample_name_in_correct_format_then_correct_sample_loaded():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
    assert tasks[0].name == "robot_load"
    assert tasks[0].params["position"] == "8"
    assert tasks[0].params["puck"] == "1"


def test_sample_centre_uses_expected_params():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
    assert tasks[1].name == "centre_sample"
    assert tasks[1].params == {
        "start_z": -20,
        "end_z": 0,
        "steps": 20,
        "exposure_time": 0.01,
        "metadata": {
            "experiment_definition": ExperimentDefinition(
                name=" ", id="", data={"time_per_pdf": 100}
            ),
            "sample": Sample(name="test_8_1", id="", data={}),
        },
    }


def test_session_and_number_of_tasks_per_experiment_is_expected():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
    assert len(tasks) == 4
    for task in tasks:
        assert task.instrument_session == "cm12345-1"


def test_experiment_with_correct_experiment_type_are_converted():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
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
        user=None,
    )
    call_list = I151Converter().construct_blueapi_calls([task], [], [])
    assert len(call_list) == 4


def test_experiment_with_no_temperatures_runs_a_room_temperature_collection():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="", id="", data={"time_per_pdf": 100}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
    assert tasks[2].name == "data_collection"
    assert tasks[2].params["full_collection_time"] == 100
    assert tasks[2].params["exposure_time_per_frame"] == 0.1
    assert tasks[2].params["metadata"] == {
        "experiment_definition": ExperimentDefinition(
            name="", id="", data={"time_per_pdf": 100}
        ),
        "sample": Sample(name="test_8_1", id="", data={}),
    }


def test_experiment_with_temperatures_runs_a_blower_collection():
    experiment_definition = ExperimentDefinition(
        name="",
        id="",
        data={
            "list_of_temperatures": [100, 120],
            "time_per_pdf": 100,
            "settle_time": 5,
            "ramp_rate": 10,
        },
    )

    experiment = Experiment(
        name="test_experiment",
        experiment_definition=experiment_definition,
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
    assert tasks[2].name == "blower_collection"
    assert tasks[2].params["time_per_collection"] == 100
    assert tasks[2].params["exposure_time_per_frame"] == 0.1
    assert tasks[2].params["ramp_rate_c_per_min"] == 10
    assert tasks[2].params["settle_time"] == 5
    assert tasks[2].params["temperatures_celsius"] == [100, 120]
    assert tasks[2].params["metadata"] == {
        "experiment_definition": experiment_definition,
        "sample": Sample(name="test_8_1", id="", data={}),
    }


def test_mix_of_experiments_with_correct_experiment_type_are_converted():
    good_experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
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
        user=None,
    )

    class BadExperiment:
        instrument_session = "cm12345-1"

    bad_task = deepcopy(good_task)
    bad_task.experiment = BadExperiment()  # type: ignore
    plan_task = deepcopy(good_task)
    plan_task.experiment = TaskRequest(name="", instrument_session="")
    plan_task.kind = TaskKind.PLAN
    tasks = [good_task, bad_task, plan_task, good_task]
    call_list = I151Converter().construct_blueapi_calls(tasks, [], [])
    assert len(call_list) == 9


def test_if_no_background_found_in_tiled_then_background_scan_added_to_tasks(
    background_not_found_in_tiled: None,
):
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
        ),
        sample=Sample(name="test_8_1", id="", data={}),
        instrument_session="cm12345-1",
    )
    task = Task(
        experiment=experiment,
        id="1",
    )
    tasks = I151Converter().pre_process([task], [], [])
    assert len(tasks) == 2
    tasks[0].id = ""
    assert tasks[0].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "cm12345-1",
            "sample": {"name": "fq_1_1", "id": "", "data": {}},
            "experiment_definition": {
                "name": "background_scan",
                "id": "",
                "data": {"background": {"bg_type": "fq"}},
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }


def test_add_required_background_scans_does_not_add_the_same_background_twice(
    tasks: list[Task], background_not_found_in_tiled: None
):
    bg_1 = BackgroundInfo(bg_type="air")
    bg_2 = BackgroundInfo(bg_type="bs")
    bg_3 = BackgroundInfo(bg_type="fq")

    def fake_get_required_background(self: I151Converter, experiment: Experiment):
        # Get the same background scans every other experiment
        # Only one of each background should be added
        if int(experiment.sample.id) % 2 == 0:
            return [bg_1, bg_2]
        else:
            return [bg_3]

    assert len(tasks) == 5
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.I151Converter._get_required_backgrounds",
        fake_get_required_background,
    ):
        new_tasks = I151Converter()._add_required_background_scans(tasks)

    assert len(new_tasks) == 8

    assert_tasks_equal(
        new_tasks[0],
        Task(
            experiment=I151Converter()._construct_background_experiment(
                bg_1, instrument_session=""
            ),
        ),
    )
    assert_tasks_equal(
        new_tasks[1],
        Task(
            experiment=I151Converter()._construct_background_experiment(
                bg_2, instrument_session=""
            ),
        ),
    )
    # This one placed before the task that requires it
    assert_tasks_equal(
        new_tasks[3],
        Task(
            experiment=I151Converter()._construct_background_experiment(
                bg_3, instrument_session=""
            ),
        ),
    )


def test_same_experiment_in_different_instrument_sessions_will_add_background_in_each(
    tasks: list[Task], background_not_found_in_tiled: None
):
    tasks[1].experiment.instrument_session = "different"

    assert len(tasks) == 5

    new_tasks = I151Converter()._add_required_background_scans(tasks)

    assert len(new_tasks) == 7
    new_tasks[0].id = ""
    assert new_tasks[0].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "",
            "sample": {"name": "fq_1_1", "id": "", "data": {}},
            "experiment_definition": {
                "name": "background_scan",
                "id": "",
                "data": {"background": {"bg_type": "fq"}},
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }
    new_tasks[2].id = ""
    assert new_tasks[2].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "different",
            "sample": {"name": "fq_1_1", "id": "", "data": {}},
            "experiment_definition": {
                "name": "background_scan",
                "id": "",
                "data": {"background": {"bg_type": "fq"}},
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }


def test_add_required_background_scans_if_found_in_tiled_then_no_background_added(
    tasks: list[Task],
    background_found_in_tiled: None,
):
    tasks_after = I151Converter()._add_required_background_scans(tasks)
    assert tasks_after == tasks


@pytest.mark.parametrize(
    "params, tiled_ids, backgrounds, expected_params",
    [
        (
            {"sample": "my_sample"},
            ["tiled_id"],
            [BackgroundInfo(bg_type="bs")],
            {
                "metadata": {
                    "tiled_backgrounds": {"tiled_id": BackgroundInfo(bg_type="bs")}
                },
                "sample": "my_sample",
            },
        ),
        (
            {},
            ["tiled_id"],
            [BackgroundInfo(bg_type="bs")],
            {
                "metadata": {
                    "tiled_backgrounds": {"tiled_id": BackgroundInfo(bg_type="bs")}
                },
            },
        ),
        (
            {"sample": "my_sample"},
            ["tiled_id_1", "tiled_id_2"],
            [
                BackgroundInfo(bg_type="bs"),
                BackgroundInfo(bg_type="air"),
            ],
            {
                "metadata": {
                    "tiled_backgrounds": {
                        "tiled_id_1": BackgroundInfo(bg_type="bs"),
                        "tiled_id_2": BackgroundInfo(bg_type="air"),
                    }
                },
                "sample": "my_sample",
            },
        ),
    ],
)
def test_add_tiled_background_to_md_adds_expected_metadata(
    params: dict[str, Any],
    tiled_ids: list[str],
    backgrounds: list[BackgroundInfo],
    expected_params: dict[str, Any],
):
    for tiled_id, background in zip(tiled_ids, backgrounds, strict=True):
        I151Converter()._add_tiled_background_to_md(params, tiled_id, background)

    assert params == expected_params
