import json
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskError, TaskResult
from daq_config_server.models.i15_1.queue_parameters import QueueParameters
from daq_config_server.models.i15_1.standards_puck import (
    STANDARD_CAPILLARY,
    STANDARD_SAMPLE,
    StandardsPin,
    StandardsPuck,
)
from heliotrapi.client import AnalysisResult

from daq_queuing_service.broadcaster import Broadcaster, serialise
from daq_queuing_service.plugins.i15_1.auxiliary import (
    AuxiliaryScan,
    AuxiliaryScanType,
    TiledAuxiliary,
)
from daq_queuing_service.plugins.i15_1.i15_1_converter import I151Converter
from daq_queuing_service.plugins.i15_1.plans import ScanType
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.task_queue.task import (
    Experiment,
    ExperimentDefinition,
    Status,
    Task,
    TaskKind,
    TaskWithPosition,
)

from ...conftest import make_sample


@pytest.fixture
def i15_1_tasks():
    time_per_pdfs = [5, 10, 10, 20, 25]
    tasks = [
        Task(
            experiment=Experiment(
                name=f"task_{i}",
                instrument_session="cm12345-1",
                experiment_definition=ExperimentDefinition(
                    name="",
                    id="",
                    data={
                        "list_of_temperatures": [100 * i, 100 * i + 20],
                        "time_per_pdf": time_per_pdfs[i],
                        "settle_time": 5,
                        "ramp_rate": 10,
                    },
                ),
                sample=make_sample(f"sample_{i}_2", id=str(i)),
            )
        )
        for i in range(5)
    ]
    return tasks


def make_background_task(capillary: STANDARD_CAPILLARY, time_per_pdf: int) -> Task:
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary=capillary, contents=None),
        time_per_pdf=time_per_pdf,
    )
    return Task(
        experiment=I151Converter()._construct_auxiliary_experiment(
            background, "cm12345-1"
        )
    )


def assert_tasks_equal(task1: Task | TaskWithPosition, task2: Task | TaskWithPosition):
    # Check two tasks are equal other than the generated UUID
    copy1 = type(task1).model_validate(task1)
    copy2 = type(task2).model_validate(task2)
    copy1.id = copy2.id = ""
    assert task1 == task2


def create_task_dict(task_id: str, experiment: dict[str, Any]) -> dict[str, Any]:
    return {
        "experiment": experiment,
        "id": task_id,
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }


def air_task(
    task_id: str, instrument_session: str, time_per_pdf: float
) -> dict[str, Any]:
    return create_task_dict(
        task_id,
        {
            "name": "Air",
            "instrument_session": instrument_session,
            "sample": None,
            "experiment_definition": {
                "name": "Air",
                "id": "",
                "data": {"time_per_pdf": time_per_pdf},
            },
        },
    )


def empty_capillary_task(
    task_id: str,
    instrument_session: str,
    time_per_pdf: float,
    capillary: STANDARD_CAPILLARY,
    standards_puck: StandardsPuck,
) -> dict[str, Any]:
    return create_task_dict(
        task_id,
        {
            "name": f"Empty {capillary} - Empty Capillary",
            "instrument_session": instrument_session,
            "sample": {
                "name": f"Empty {capillary}",
                "id": "",
                "data": {"capillary": capillary, "composition": None},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": standards_puck.get_position_of_pin(
                        StandardsPin(capillary=capillary, contents=None)
                    ),
                },
            },
            "experiment_definition": {
                "name": "Empty Capillary",
                "id": "",
                "data": {
                    "time_per_pdf": time_per_pdf,
                },
            },
        },
    )


def standard_sample_task(
    task_id: str,
    instrument_session: str,
    time_per_pdf: float,
    capillary: STANDARD_CAPILLARY,
    contents: STANDARD_SAMPLE,
    standards_puck: StandardsPuck,
) -> dict[str, Any]:
    return create_task_dict(
        task_id,
        {
            "name": f"{contents} {capillary} - Standard Sample",
            "instrument_session": instrument_session,
            "sample": {
                "name": f"{contents} {capillary}",
                "id": "",
                "data": {"capillary": capillary, "composition": contents},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": standards_puck.get_position_of_pin(
                        StandardsPin(capillary=capillary, contents=contents)
                    ),
                },
            },
            "experiment_definition": {
                "name": "Standard Sample",
                "id": "",
                "data": {
                    "time_per_pdf": time_per_pdf,
                },
            },
        },
    )


@pytest.fixture
async def queue_with_i15_1_plugin(
    i15_1_tasks: list[Task], background_not_found_in_tiled: MagicMock
):
    queue = TaskQueue(converter=I151Converter(), broadcaster=Broadcaster())

    await queue.add_tasks(i15_1_tasks)
    await queue.resume_queue()
    return queue


@pytest.fixture(autouse=True)
def background_found_in_tiled():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_suitable_tiled_scan",
        MagicMock(
            return_value=TiledAuxiliary(
                instrument_session="cm12345-1",
                tiled_id="fake_tiled_id",
                pin=StandardsPin(capillary="fq1.0", contents=None),
                time_per_pdf=5,
                filepath=Path(""),
                instrument_session_directory=Path(""),
                filename="",
            )
        ),
    ) as mock_get_suitable_tiled_background:
        yield mock_get_suitable_tiled_background


@pytest.fixture()
def background_not_found_in_tiled():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_suitable_tiled_scan",
        MagicMock(return_value=None),
    ) as mock_get_suitable_tiled_background:
        yield mock_get_suitable_tiled_background


@pytest.fixture
def i15_1_converter():
    converter = I151Converter()
    converter._tiled_client = MagicMock()
    return converter


def test_given_sample_name_in_correct_format_then_correct_sample_loaded():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[0].name == "robot_load"
    assert tasks[0].params["position"] == 2
    assert tasks[0].params["puck"] == 2


def test_centre_sample_at_room_temperature_uses_correct_params_from_queue_params(
    queue_params: QueueParameters,
):
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    centring_params = queue_params.room_temperature_centring_params
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[2].name == "centre_sample"
    assert tasks[2].params == {
        "start_z": centring_params.start_z,
        "end_z": centring_params.end_z,
        "steps": 270,
        "exposure_time": centring_params.exposure_time_per_frame_s,
        "metadata": {
            "experiment_definition": ExperimentDefinition(
                name=" ", id="", data={"time_per_pdf": 100}
            ),
            "sample": make_sample("test_8_1", ""),
        },
    }


def test_centre_sample_with_blower_uses_correct_params_from_queue_params(
    queue_params: QueueParameters,
):
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ",
            id="",
            data={
                "list_of_temperatures": [100, 120],
                "time_per_pdf": 100,
                "settle_time": 5,
                "ramp_rate": 10,
            },
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[2].name == "centre_sample"
    assert tasks[2].params == {
        "start_z": queue_params.blower_centring_params.start_z,
        "end_z": queue_params.blower_centring_params.end_z,
        "steps": 150,
        "exposure_time": queue_params.blower_centring_params.exposure_time_per_frame_s,
        "metadata": {
            "experiment_definition": ExperimentDefinition(
                name=" ",
                id="",
                data={
                    "list_of_temperatures": [100, 120],
                    "time_per_pdf": 100,
                    "settle_time": 5,
                    "ramp_rate": 10,
                },
            ),
            "sample": make_sample("test_8_1", ""),
        },
    }


def test_session_and_number_of_tasks_per_experiment_is_expected():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert len(tasks) == 6
    for task in tasks:
        assert task.instrument_session == "cm12345-1"


def test_experiment_with_correct_experiment_type_are_converted():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
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
    assert len(call_list) == 6


def test_experiment_with_no_temperatures_runs_a_room_temperature_collection():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[4].name == "data_collection"
    assert tasks[4].params["full_collection_time"] == 100
    assert tasks[4].params["exposure_time_per_frame"] == 1.0
    assert tasks[4].params["scan_type"] == ScanType.DATA_COLLECTION
    assert tasks[4].params["metadata"] == {
        "experiment_definition": ExperimentDefinition(
            name="", id="", data={"time_per_pdf": 100}
        ),
        "sample": make_sample("test_8_1", ""),
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
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[4].name == "blower_collection"
    assert tasks[4].params["time_per_collection"] == 100
    assert tasks[4].params["exposure_time_per_frame"] == 1.0
    assert tasks[4].params["ramp_rate_c_per_min"] == 10
    assert tasks[4].params["settle_time"] == 5
    assert tasks[4].params["temperatures_celsius"] == [100, 120]
    assert tasks[4].params["scan_type"] == ScanType.DATA_COLLECTION
    assert tasks[4].params["metadata"] == {
        "experiment_definition": experiment_definition,
        "sample": make_sample("test_8_1", ""),
    }


def test_experiment_with_no_sample_converts_to_air_scan():
    experiment_definition = ExperimentDefinition(
        name="",
        id="",
        data={"time_per_pdf": 100},
    )

    experiment = Experiment(
        name="test_experiment",
        experiment_definition=experiment_definition,
        sample=None,
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert len(tasks) == 2
    assert tasks[0].name == "wait_for_beam"
    assert tasks[1].name == "data_collection"
    assert tasks[1].params == {
        "full_collection_time": 100,
        "exposure_time_per_frame": 1.0,
        "scan_type": ScanType.DATA_COLLECTION,
        "metadata": {
            "sample": None,
            "experiment_definition": ExperimentDefinition(
                name="", id="", data={"time_per_pdf": 100}
            ),
        },
    }


def test_wait_for_beam_plans_added_by_converter():
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
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
    for i in (1, 3):
        assert tasks[i].name == "wait_for_beam"


def test_tiled_backgrounds_added_to_metadata_if_present():
    converter = I151Converter()
    converter._tiled_auxiliary_scans["id"] = {
        AuxiliaryScanType.EMPTY_CAPILLARY: TiledAuxiliary(
            instrument_session="cm12345-1",
            tiled_id="tiled_id",
            pin=StandardsPin(capillary="bs1.5", contents=None),
            time_per_pdf=1,
            filepath=Path(""),
            instrument_session_directory=Path(""),
            filename="",
        )
    }
    experiment_definition = ExperimentDefinition(
        name="",
        id="",
        data={"time_per_pdf": 100},
    )

    experiment = Experiment(
        name="test_experiment",
        experiment_definition=experiment_definition,
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = converter._construct_blueapi_tasks_from_experiment(experiment, "id")
    assert tasks[4].params["metadata"] == {
        "experiment_definition": experiment_definition,
        "sample": make_sample("test_8_1", ""),
        "auxiliary_scans": {
            AuxiliaryScanType.EMPTY_CAPILLARY: TiledAuxiliary(
                instrument_session="cm12345-1",
                pin=StandardsPin(capillary="bs1.5", contents=None),
                time_per_pdf=1,
                tiled_id="tiled_id",
                filepath=Path(""),
                instrument_session_directory=Path(""),
                filename="",
            ),
        },
    }


def test_mix_of_experiments_with_correct_experiment_type_are_converted():
    good_experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
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
    assert len(call_list) == 13


def test_if_no_background_found_in_tiled_then_background_scan_added_to_tasks(
    background_not_found_in_tiled: MagicMock, standards_puck: StandardsPuck
):
    converter = I151Converter()
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name="run_full_collection", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    task = Task(
        experiment=experiment,
        id="1",
    )
    tasks = converter.pre_process(None, [task], [], [])
    assert len(tasks) == 4
    assert tasks[0].model_dump() == air_task(tasks[0].id, "cm12345-1", 100.0)
    assert tasks[1].model_dump() == empty_capillary_task(
        tasks[1].id, "cm12345-1", 100.0, "fq1.0", standards_puck
    )
    assert tasks[2].model_dump() == standard_sample_task(
        tasks[2].id, "cm12345-1", 100.0, "fq1.0", "Silicon", standards_puck
    )
    assert converter._tiled_auxiliary_scans == {"1": {}}


def test_add_required_background_scans_does_not_add_the_same_background_twice(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
):
    bg_1 = AuxiliaryScan(instrument_session="cm12345-1", pin=None, time_per_pdf=5)
    bg_2 = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="bs1.0", contents=None),
        time_per_pdf=10,
    )
    bg_3 = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=15,
    )

    def fake_get_required_auxiliary_scans(self: I151Converter, experiment: Experiment):
        # Get the same background scans every other experiment
        # Only one of each background should be added
        assert experiment.sample
        if int(experiment.sample.id) % 2 == 0:
            return [bg_1, bg_2]
        else:
            return [bg_3]

    assert len(i15_1_tasks) == 5
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.I151Converter._get_required_auxiliary_scans",
        fake_get_required_auxiliary_scans,
    ):
        new_tasks = i15_1_converter._add_required_auxiliary_scans(None, i15_1_tasks)

    assert len(new_tasks) == 8

    assert_tasks_equal(
        new_tasks[0],
        Task(
            experiment=I151Converter()._construct_auxiliary_experiment(
                bg_1, instrument_session="cm12345-1"
            ),
        ),
    )
    assert_tasks_equal(
        new_tasks[1],
        Task(
            experiment=I151Converter()._construct_auxiliary_experiment(
                bg_2, instrument_session="cm12345-1"
            ),
        ),
    )
    # This one placed before the task that requires it
    assert_tasks_equal(
        new_tasks[3],
        Task(
            experiment=I151Converter()._construct_auxiliary_experiment(
                bg_3, instrument_session="cm12345-1"
            ),
        ),
    )


def test_add_required_background_scans_combines_similar_background_requirements(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
):

    assert len(i15_1_tasks) == 5
    new_tasks = i15_1_converter._add_required_auxiliary_scans(None, i15_1_tasks)
    assert len(new_tasks) == 8
    assert (
        isinstance(new_tasks[0].experiment, Experiment)
        and new_tasks[0].experiment.sample is None
    )
    assert (
        isinstance(new_tasks[1].experiment, Experiment)
        and new_tasks[1].experiment.sample
        and new_tasks[1].experiment.sample.name == "Empty fq1.0"
    )
    assert (
        isinstance(new_tasks[2].experiment, Experiment)
        and new_tasks[2].experiment.sample
        and new_tasks[2].experiment.sample.name == "Silicon fq1.0"
    )
    # Should have the maximum time_per_pdf of i15_1_tasks
    assert new_tasks[0].experiment.experiment_definition.data["time_per_pdf"] == 25
    assert new_tasks[1].experiment.experiment_definition.data["time_per_pdf"] == 25
    assert new_tasks[2].experiment.experiment_definition.data["time_per_pdf"] == 25


def test_same_experiment_in_different_instrument_sessions_will_add_backgrounds_in_each(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
    standards_puck: StandardsPuck,
):
    i15_1_tasks[1].experiment.instrument_session = "different"
    i15_1_tasks[2].experiment.instrument_session = "also_different"

    assert len(i15_1_tasks) == 5

    new_tasks = i15_1_converter._add_required_auxiliary_scans(None, i15_1_tasks)

    assert len(new_tasks) == 14

    assert new_tasks[0].model_dump() == air_task(new_tasks[0].id, "cm12345-1", 25.0)
    assert new_tasks[1].model_dump() == empty_capillary_task(
        new_tasks[1].id, "cm12345-1", 25.0, "fq1.0", standards_puck
    )
    assert new_tasks[2].model_dump() == standard_sample_task(
        new_tasks[2].id, "cm12345-1", 25.0, "fq1.0", "Silicon", standards_puck
    )

    assert new_tasks[4].model_dump() == air_task(new_tasks[4].id, "different", 10.0)
    assert new_tasks[5].model_dump() == empty_capillary_task(
        new_tasks[5].id, "different", 10.0, "fq1.0", standards_puck
    )
    assert new_tasks[6].model_dump() == standard_sample_task(
        new_tasks[6].id, "different", 10.0, "fq1.0", "Silicon", standards_puck
    )

    assert new_tasks[8].model_dump() == air_task(
        new_tasks[8].id, "also_different", 10.0
    )
    assert new_tasks[9].model_dump() == empty_capillary_task(
        new_tasks[9].id, "also_different", 10.0, "fq1.0", standards_puck
    )
    assert new_tasks[10].model_dump() == standard_sample_task(
        new_tasks[10].id, "also_different", 10.0, "fq1.0", "Silicon", standards_puck
    )


def test_add_required_background_scans_if_found_in_tiled_then_no_background_added(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_found_in_tiled: MagicMock,
):
    assert i15_1_converter._tiled_auxiliary_scans == {}

    tasks_after = i15_1_converter._add_required_auxiliary_scans(None, i15_1_tasks)

    assert tasks_after == i15_1_tasks
    # Tiled backgrounds info should be saved in state
    assert len(i15_1_converter._tiled_auxiliary_scans.keys()) == 5
    assert i15_1_converter._tiled_auxiliary_scans == {
        task.id: {
            AuxiliaryScanType.EMPTY_CAPILLARY: TiledAuxiliary(
                instrument_session="cm12345-1",
                pin=StandardsPin(capillary="fq1.0", contents=None),
                time_per_pdf=5,
                tiled_id="fake_tiled_id",
                filepath=Path(""),
                instrument_session_directory=Path(""),
                filename="",
            )
        }
        for task in i15_1_tasks
    }


async def test_queue_with_i15_1_converter_can_sync(queue_with_i15_1_plugin: TaskQueue):
    first_task = await queue_with_i15_1_plugin.get_task_by_position(0)
    assert first_task
    await queue_with_i15_1_plugin.move_task(first_task.id, 2)


def test__ensure_background_in_queue_or_tiled_returns_if_suitable_already_queued(
    i15_1_converter: I151Converter, background_not_found_in_tiled: MagicMock
):
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=25,
    )
    new_tasks = [make_background_task("fq1.0", 25)]
    result = i15_1_converter._ensure_auxiliary_in_queue_or_tiled(
        background, None, new_tasks, "task_id", "cm12345-1"
    )
    assert result == new_tasks


def test__ensure_background_in_queue_or_tiled_returns_if_current_task_is_suitable(
    i15_1_converter: I151Converter,
):
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=25,
    )
    new_tasks: list[Task] = []
    result = i15_1_converter._ensure_auxiliary_in_queue_or_tiled(
        background,
        TaskWithPosition.from_task(make_background_task("fq1.0", 25)),
        new_tasks,
        "task_id",
        "cm12345-1",
    )
    assert result == new_tasks


def test__ensure_background_in_queue_or_tiled_modifies_queued_background_if_possible(
    i15_1_converter: I151Converter, background_not_found_in_tiled: MagicMock
):
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=25,
    )
    current_task = TaskWithPosition.from_task(make_background_task("fq1.0", 25))
    # Current task is suitable but not a background
    assert isinstance(current_task.experiment, Experiment)
    current_task.experiment.experiment_definition.name = "Not a background"
    new_tasks = [make_background_task("fq1.0", 10)]
    result = i15_1_converter._ensure_auxiliary_in_queue_or_tiled(
        background, current_task, new_tasks, "task_id", "cm12345-1"
    )
    assert len(result) == 1
    assert_tasks_equal(result[0], make_background_task("fq1.0", 25))


def test__ensure_background_in_queue_or_tiled_adds_background_if_none_suitable_in_queue(
    i15_1_converter: I151Converter,
    background_not_found_in_tiled: MagicMock,
):
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=25,
    )
    result = i15_1_converter._ensure_auxiliary_in_queue_or_tiled(
        background, None, [], "task_id", "cm12345-1"
    )
    assert len(result) == 1
    assert_tasks_equal(result[0], make_background_task("fq1.0", 25))


def test__ensure_background_in_queue_or_tiled_saves_tiled_info_if_exists(
    i15_1_converter: I151Converter,
    background_found_in_tiled: MagicMock,
):
    i15_1_converter._tiled_auxiliary_scans["task_id"] = {}
    background = AuxiliaryScan(
        instrument_session="cm12345-1",
        pin=StandardsPin(capillary="fq1.0", contents=None),
        time_per_pdf=25,
    )
    result = i15_1_converter._ensure_auxiliary_in_queue_or_tiled(
        background, None, [], "task_id", "cm12345-1"
    )
    assert len(result) == 0
    assert i15_1_converter._tiled_auxiliary_scans == {
        "task_id": {
            AuxiliaryScanType.EMPTY_CAPILLARY: TiledAuxiliary(
                instrument_session="cm12345-1",
                pin=StandardsPin(capillary="fq1.0", contents=None),
                time_per_pdf=5,
                tiled_id="fake_tiled_id",
                filepath=Path(""),
                instrument_session_directory=Path(""),
                filename="",
            )
        }
    }


@pytest.mark.parametrize(
    "index, expected_scan_type",
    [  # 0 is wait_for_beam
        (1, ScanType.AIR),
        # 2 is robot load
        # 3 is wait_for_beam
        # 4 is centring
        # 5 is wait_for_beam
        (6, ScanType.EMPTY_CAPILLARY),
        # 7 is robot unload
        # 8 is robot load
        # 9 is wait_for_beam
        # 10 is centring
        # 11 is wait_for_beam
        (12, ScanType.STANDARD_SAMPLE),
        (18, ScanType.DATA_COLLECTION),
    ],
)
async def test_queued_data_collections_have_correct_scan_type_parameter(
    index: int, expected_scan_type: ScanType, queue_with_i15_1_plugin: TaskQueue
):
    blueapi_calls = await queue_with_i15_1_plugin.get_call_queue()
    assert blueapi_calls[index].task_request.params["scan_type"] == expected_scan_type


def test_i15_1_tasks_can_be_serialised():
    converter = I151Converter()
    converter._tiled_auxiliary_scans["id"] = {
        AuxiliaryScanType.EMPTY_CAPILLARY: TiledAuxiliary(
            instrument_session="cm12345-1",
            tiled_id="tiled_id",
            pin=StandardsPin(capillary="bs1.0", contents=None),
            time_per_pdf=1,
            filepath=Path(""),
            instrument_session_directory=Path(""),
            filename="",
        )
    }
    experiment_definition = ExperimentDefinition(
        name="",
        id="",
        data={"time_per_pdf": 100},
    )

    experiment = Experiment(
        name="test_experiment",
        experiment_definition=experiment_definition,
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = converter._construct_blueapi_tasks_from_experiment(experiment, "id")
    serialised = serialise(tasks)
    json.dumps(serialised)


def test_i15_1_converter_does_not_pause_queue_if_sample_not_found_error():
    converter = I151Converter()
    should_pause = converter.pause_on_fail_blueapi_call(
        MagicMock(),
        [
            TaskError(
                type="",
                message="Robot load failed, no sample found at puck 1, position 15",
            )
        ],
    )
    assert should_pause is False


def test_no_auxiliary_scans_added_if_turned_off_in_queue_params(
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
    queue_params: QueueParameters,
):
    queue_params.add_auxiliary_scans = False
    task = i15_1_tasks[0]
    new_tasks = I151Converter().pre_process(None, [task], [], [])
    assert len(new_tasks) == 1
    assert new_tasks == [task]


def test_no_robot_load_queued_if_turned_off_in_queue_params(
    i15_1_tasks: list[Task], queue_params: QueueParameters
):
    queue_params.add_auxiliary_scans = False
    queue_params.do_robot_load = False

    blueapi_calls = I151Converter().construct_blueapi_calls(
        [TaskWithPosition.from_task(i15_1_tasks[0])], [], []
    )
    assert not any(call.task_request.name == "robot_load" for call in blueapi_calls)
    assert not any(call.task_request.name == "robot_unload" for call in blueapi_calls)


def test_no_centring_queued_if_turned_off_in_queue_params(
    i15_1_tasks: list[Task], queue_params: QueueParameters
):
    queue_params.add_auxiliary_scans = False
    queue_params.do_centring = False

    blueapi_calls = I151Converter().construct_blueapi_calls(
        [TaskWithPosition.from_task(i15_1_tasks[0])], [], []
    )
    assert not any(call.task_request.name == "centre_sample" for call in blueapi_calls)
    wait_for_beams = [
        call for call in blueapi_calls if call.task_request.name == "wait_for_beam"
    ]
    assert len(wait_for_beams) == 1  # Just before data collection


def test_no_wait_for_beam_queued_if_turned_off_in_queue_params(
    i15_1_tasks: list[Task], queue_params: QueueParameters
):
    queue_params.add_auxiliary_scans = False
    queue_params.do_wait_for_beam = False

    blueapi_calls = I151Converter().construct_blueapi_calls(
        [TaskWithPosition.from_task(i15_1_tasks[0])], [], []
    )
    assert not any(call.task_request.name == "wait_for_beam" for call in blueapi_calls)


async def test_centring_info_added_to_data_collection_metadata(
    queue_with_i15_1_plugin: TaskQueue, centring_result: AnalysisResult
):
    while True:
        call = await queue_with_i15_1_plugin.get_next_call_once_available()
        if (
            call.task_request.name == "data_collection"
            and call.task_request.params["metadata"].get("sample") is not None
        ):
            break
        call.put_in_progress()
        result = (
            centring_result.model_dump(mode="json")
            if call.task_request.name == "centre_sample"
            else None
        )
        await queue_with_i15_1_plugin.complete_call(
            call, TaskResult(outcome="success", result=result, type="AnalysisResult")
        )

    assert call.task_request.params["metadata"]["centring_scan"] == {
        "result": TaskResult(
            outcome="success",
            result={
                "request_id": "7d71e99e-f6f6-4e93-a1e4-ac91f8804006",
                "status": "completed",
                "analysis_name": "sample_alignment_i15_1",
                "inputs": {
                    "filepath": "/dls/i15-1/data/2026/cm44163-4/i15-1-100259.nxs",
                    "beamline": "i15-1",
                },
                "result": {
                    "centre": -1.7327164569369127,
                    "peaks": [
                        {
                            "amplitude": 684.5096633867339,
                            "centre": -12.500470871238504,
                            "fwhm": 16.126336919254836,
                            "normalised": False,
                        },
                        {
                            "amplitude": 14080.218114003073,
                            "centre": -1.7327164569369127,
                            "fwhm": 7.5161854303657645,
                            "normalised": False,
                        },
                    ],
                    "scores": [11038.633456260188, 105829.53024484203],
                },
                "created_at": "2026-10-12T22:22:45.370672",
                "finished_at": "2026-10-12T22:22:47.577230",
                "version": "0.3.4.dev0+gfbb23e084.d20260929",
            },
            type="AnalysisResult",
        ),
        "tiled_id": None,
    }
