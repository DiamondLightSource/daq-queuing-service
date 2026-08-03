from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo
from daq_queuing_service.plugins.i15_1.i15_1_converter import I151Converter
from daq_queuing_service.task import (
    Experiment,
    ExperimentDefinition,
    Sample,
    Status,
    Task,
    TaskKind,
    TaskWithPosition,
)


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
        experiment_definition=ExperimentDefinition(name=" ", id="", data={}),
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
        experiment_definition=ExperimentDefinition(name=" ", id="", data={}),
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
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment)
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
    call_list = I151Converter().construct_blueapi_calls([task], [], [])
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

    class BadExperiment:
        instrument_session = "cm12345-1"

    bad_task = deepcopy(good_task)
    bad_task.experiment = BadExperiment()  # type: ignore
    plan_task = deepcopy(good_task)
    plan_task.experiment = TaskRequest(name="", instrument_session="")
    plan_task.kind = TaskKind.PLAN
    tasks = [good_task, bad_task, plan_task, good_task]
    call_list = I151Converter().construct_blueapi_calls(tasks, [], [])
    assert len(call_list) == 7


def test_if_no_background_found_in_tiled_then_background_scan_added_to_call_list(
    background_not_found_in_tiled: None,
):
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
    tasks = I151Converter().construct_blueapi_calls([task], [], [])
    assert len(tasks) == 4
    assert tasks[0] == BlueapiCall(
        task_request=TaskRequest(
            name="static_collection",
            params={
                "metadata": {
                    "background": BackgroundInfo(
                        bg_type="air", cobra=False, blower=False
                    )
                }
            },
            instrument_session="cm12345-1",
        ),
        parent_task_id="1",
    )


def test_add_required_background_scans_does_not_add_the_same_background_twice(
    tasks_and_calls: tuple[list[TaskWithPosition], list[BlueapiCall]],
    background_not_found_in_tiled: None,
):
    tasks, calls = tasks_and_calls
    bg_1 = BackgroundInfo(bg_type="air", cobra=False, blower=False)
    bg_2 = BackgroundInfo(bg_type="capillary_1", cobra=True, blower=False)
    bg_3 = BackgroundInfo(bg_type="capillary_1", cobra=False, blower=True)

    def fake_get_required_background(self: I151Converter, experiment: Experiment):
        # Get the same background scans every other experiment
        # Only one of each background should be added
        if int(experiment.sample.id) % 2 == 0:
            return [bg_1, bg_2]
        else:
            return [bg_3]

    assert len(calls) == 15
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.I151Converter._get_required_backgrounds",
        fake_get_required_background,
    ):
        new_calls = I151Converter()._add_required_background_scans(tasks, calls)

    assert len(new_calls) == 18

    assert new_calls[0] == BlueapiCall(
        task_request=I151Converter()._construct_background_task_request(
            bg_1, instrument_session=""
        ),
        parent_task_id="0",
    )
    assert new_calls[1] == BlueapiCall(
        task_request=I151Converter()._construct_background_task_request(
            bg_2, instrument_session=""
        ),
        parent_task_id="0",
    )
    # This one placed before the task that requires it
    assert new_calls[5] == BlueapiCall(
        task_request=I151Converter()._construct_background_task_request(
            bg_3, instrument_session=""
        ),
        parent_task_id="1",
    )


def test_same_experiment_in_different_instrument_sessions_will_add_background_in_each(
    tasks_and_calls: tuple[list[TaskWithPosition], list[BlueapiCall]],
    background_not_found_in_tiled: None,
):
    tasks, _ = tasks_and_calls
    tasks[1].experiment.instrument_session = "different"
    calls: list[BlueapiCall] = []
    for task in tasks:
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

    assert len(calls) == 15

    new_calls = I151Converter()._add_required_background_scans(tasks, calls)

    assert len(new_calls) == 17
    assert new_calls[0] == BlueapiCall(
        task_request=TaskRequest(
            name="static_collection",
            params={
                "metadata": {
                    "background": BackgroundInfo(
                        bg_type="air", cobra=False, blower=False
                    )
                }
            },
            instrument_session="",
        ),
        parent_task_id="0",
    )
    assert new_calls[4] == BlueapiCall(
        task_request=TaskRequest(
            name="static_collection",
            params={
                "metadata": {
                    "background": BackgroundInfo(
                        bg_type="air", cobra=False, blower=False
                    )
                }
            },
            instrument_session="different",
        ),
        parent_task_id="1",
    )


def test_add_required_background_scans_if_found_in_tiled_then_no_background_added(
    tasks_and_calls: tuple[list[TaskWithPosition], list[BlueapiCall]],
    background_found_in_tiled: None,
):
    tasks, calls_before = tasks_and_calls
    calls_after = I151Converter()._add_required_background_scans(tasks, calls_before)
    assert calls_after == calls_before


@pytest.mark.parametrize(
    "params, tiled_ids, backgrounds, expected_params",
    [
        (
            {"sample": "my_sample"},
            ["tiled_id"],
            [BackgroundInfo(bg_type="capillary_1", cobra=False, blower=True)],
            {
                "metadata": {
                    "tiled_backgrounds": {
                        "tiled_id": BackgroundInfo(
                            bg_type="capillary_1", cobra=False, blower=True
                        )
                    }
                },
                "sample": "my_sample",
            },
        ),
        (
            {},
            ["tiled_id"],
            [BackgroundInfo(bg_type="capillary_1", cobra=False, blower=True)],
            {
                "metadata": {
                    "tiled_backgrounds": {
                        "tiled_id": BackgroundInfo(
                            bg_type="capillary_1", cobra=False, blower=True
                        )
                    }
                },
            },
        ),
        (
            {"sample": "my_sample"},
            ["tiled_id_1", "tiled_id_2"],
            [
                BackgroundInfo(bg_type="capillary_1", cobra=False, blower=True),
                BackgroundInfo(bg_type="air", cobra=True, blower=False),
            ],
            {
                "metadata": {
                    "tiled_backgrounds": {
                        "tiled_id_1": BackgroundInfo(
                            bg_type="capillary_1", cobra=False, blower=True
                        ),
                        "tiled_id_2": BackgroundInfo(
                            bg_type="air", cobra=True, blower=False
                        ),
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
