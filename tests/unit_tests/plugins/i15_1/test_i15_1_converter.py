from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.i15_1.backgrounds import (
    BACKGROUND_TYPES,
    BackgroundInfo,
    TiledBackground,
)
from daq_queuing_service.plugins.i15_1.i15_1_converter import I151Converter
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
def i15_1_tasks(tasks: list[Task]):
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


def make_background_task(bg_type: BACKGROUND_TYPES, time_per_pdf: int) -> Task:
    background = BackgroundInfo(bg_type=bg_type, time_per_pdf=time_per_pdf)
    return Task(
        experiment=I151Converter()._construct_background_experiment(
            background, "cm12345-1"
        )
    )


def assert_tasks_equal(task1: Task | TaskWithPosition, task2: Task | TaskWithPosition):
    # Check two tasks are equal other than the generated UUID
    copy1 = type(task1).model_validate(task1)
    copy2 = type(task2).model_validate(task2)
    copy1.id = copy2.id = ""
    assert task1 == task2


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
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_tiled_background",
        MagicMock(
            return_value=TiledBackground(
                tiled_id="fake_tiled_id", bg_type="fq1.0", time_per_pdf=5
            )
        ),
    ) as mock_get_tiled_background:
        yield mock_get_tiled_background


@pytest.fixture()
def background_not_found_in_tiled():
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.get_tiled_background",
        MagicMock(return_value=None),
    ) as mock_get_tiled_background:
        yield mock_get_tiled_background


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


def test_centre_sample_uses_expected_params():
    experiment = Experiment(
        name="test_experiment",
        experiment_definition=ExperimentDefinition(
            name=" ", id="", data={"time_per_pdf": 100}
        ),
        sample=make_sample("test_8_1", ""),
        instrument_session="cm12345-1",
    )
    tasks = I151Converter()._construct_blueapi_tasks_from_experiment(experiment, "id")
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
    assert len(tasks) == 4
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
    assert len(call_list) == 4


@pytest.mark.skip("Temp while gonio cant move")
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
    assert tasks[2].name == "data_collection"
    assert tasks[2].params["full_collection_time"] == 100
    assert tasks[2].params["exposure_time_per_frame"] == 0.1
    assert tasks[2].params["metadata"] == {
        "experiment_definition": ExperimentDefinition(
            name="", id="", data={"time_per_pdf": 100}
        ),
        "sample": make_sample("test_8_1", ""),
    }


@pytest.mark.skip("Temp while gonio cant move")
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
    assert tasks[2].name == "blower_collection"
    assert tasks[2].params["time_per_collection"] == 100
    assert tasks[2].params["exposure_time_per_frame"] == 0.1
    assert tasks[2].params["ramp_rate_c_per_min"] == 10
    assert tasks[2].params["settle_time"] == 5
    assert tasks[2].params["temperatures_celsius"] == [100, 120]
    assert tasks[2].params["metadata"] == {
        "experiment_definition": experiment_definition,
        "sample": make_sample("test_8_1", ""),
    }


def test_tiled_backgrounds_added_to_metadata_if_present():
    converter = I151Converter()
    converter._tiled_backgrounds["id"] = [
        TiledBackground(tiled_id="tiled_id", bg_type="pi1.0", time_per_pdf=1)
    ]
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
    assert tasks[2].params["metadata"] == {
        "experiment_definition": experiment_definition,
        "sample": make_sample("test_8_1", ""),
        "tiled_backgrounds": [
            TiledBackground(bg_type="pi1.0", time_per_pdf=1, tiled_id="tiled_id"),
        ],
        "background": False,
    }


def test_tiled_backgrounds_tagged_as_backgrounds_in_metadata():
    converter = I151Converter()
    background_task = make_background_task("air", 10)
    assert isinstance(background_task.experiment, Experiment)
    tasks = converter._construct_blueapi_tasks_from_experiment(
        background_task.experiment, "id"
    )
    assert tasks[2].params["metadata"]["background"] is True
    # Centring should not be tagged as a background scan
    assert not tasks[1].params["metadata"].get("background")


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
    assert len(call_list) == 9


def test_if_no_background_found_in_tiled_then_background_scan_added_to_tasks(
    background_not_found_in_tiled: MagicMock,
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
    tasks = converter.pre_process([task], [], [])
    assert len(tasks) == 2
    tasks[0].id = ""
    assert tasks[0].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "cm12345-1",
            "sample": {
                "name": "Empty fq1.0",
                "id": "",
                "data": {"capillary": "fq1.0"},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": 1,
                },
            },
            "experiment_definition": {
                "name": "Background",
                "id": "",
                "data": {
                    "background": {"bg_type": "fq1.0", "time_per_pdf": 100},
                    "time_per_pdf": 100,
                },
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }
    assert converter._tiled_backgrounds == {"1": []}


def test_add_required_background_scans_does_not_add_the_same_background_twice(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
):
    bg_1 = BackgroundInfo(bg_type="air", time_per_pdf=5)
    bg_2 = BackgroundInfo(bg_type="bs1.0", time_per_pdf=10)
    bg_3 = BackgroundInfo(bg_type="fq1.0", time_per_pdf=15)

    def fake_get_required_background(self: I151Converter, experiment: Experiment):
        # Get the same background scans every other experiment
        # Only one of each background should be added
        if int(experiment.sample.id) % 2 == 0:
            return [bg_1, bg_2]
        else:
            return [bg_3]

    assert len(i15_1_tasks) == 5
    with patch(
        "daq_queuing_service.plugins.i15_1.i15_1_converter.I151Converter._get_required_backgrounds",
        fake_get_required_background,
    ):
        new_tasks = i15_1_converter._add_required_background_scans(i15_1_tasks)

    assert len(new_tasks) == 8

    assert_tasks_equal(
        new_tasks[0],
        Task(
            experiment=I151Converter()._construct_background_experiment(
                bg_1, instrument_session="cm12345-1"
            ),
        ),
    )
    assert_tasks_equal(
        new_tasks[1],
        Task(
            experiment=I151Converter()._construct_background_experiment(
                bg_2, instrument_session="cm12345-1"
            ),
        ),
    )
    # This one placed before the task that requires it
    assert_tasks_equal(
        new_tasks[3],
        Task(
            experiment=I151Converter()._construct_background_experiment(
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
    new_tasks = i15_1_converter._add_required_background_scans(i15_1_tasks)
    assert len(new_tasks) == 6
    assert isinstance(new_tasks[0].experiment, Experiment)
    # Should have the maximum time_per_pdf of i15_1_tasks
    assert new_tasks[0].experiment.experiment_definition.data["time_per_pdf"] == 25


def test_same_experiment_in_different_instrument_sessions_will_add_background_in_each(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_not_found_in_tiled: MagicMock,
):
    i15_1_tasks[1].experiment.instrument_session = "different"
    i15_1_tasks[2].experiment.instrument_session = "also_different"

    assert len(i15_1_tasks) == 5

    new_tasks = i15_1_converter._add_required_background_scans(i15_1_tasks)

    assert len(new_tasks) == 8
    new_tasks[0].id = ""
    assert new_tasks[0].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "cm12345-1",
            "sample": {
                "name": "Empty fq1.0",
                "id": "",
                "data": {"capillary": "fq1.0"},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": 1,
                },
            },
            "experiment_definition": {
                "name": "Background",
                "id": "",
                "data": {
                    "background": {"bg_type": "fq1.0", "time_per_pdf": 25},
                    "time_per_pdf": 25,
                },
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
            "sample": {
                "name": "Empty fq1.0",
                "id": "",
                "data": {"capillary": "fq1.0"},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": 1,
                },
            },
            "experiment_definition": {
                "name": "Background",
                "id": "",
                "data": {
                    "background": {"bg_type": "fq1.0", "time_per_pdf": 10},
                    "time_per_pdf": 10,
                },
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }
    new_tasks[4].id = ""
    assert new_tasks[4].model_dump() == {
        "experiment": {
            "name": "Background",
            "instrument_session": "also_different",
            "sample": {
                "name": "Empty fq1.0",
                "id": "",
                "data": {"capillary": "fq1.0"},
                "container": {
                    "id": "",
                    "positionInParent": {
                        "position": 1,
                    },
                },
                "positionInContainer": {
                    "position": 1,
                },
            },
            "experiment_definition": {
                "name": "Background",
                "id": "",
                "data": {
                    "background": {"bg_type": "fq1.0", "time_per_pdf": 10},
                    "time_per_pdf": 10,
                },
            },
        },
        "id": "",
        "blueapi_calls": [],
        "status": Status.QUEUED,
        "kind": TaskKind.EXPERIMENT,
        "user": None,
    }


def test_add_required_background_scans_if_found_in_tiled_then_no_background_added(
    i15_1_converter: I151Converter,
    i15_1_tasks: list[Task],
    background_found_in_tiled: MagicMock,
):
    assert i15_1_converter._tiled_backgrounds == {}

    tasks_after = i15_1_converter._add_required_background_scans(i15_1_tasks)

    assert tasks_after == i15_1_tasks
    # Tiled backgrounds info should be saved in state
    assert len(i15_1_converter._tiled_backgrounds.keys()) == 5
    assert i15_1_converter._tiled_backgrounds == {
        task.id: [
            TiledBackground(bg_type="fq1.0", time_per_pdf=5, tiled_id="fake_tiled_id")
        ]
        for task in i15_1_tasks
    }


async def test_queue_with_i15_1_converter_can_sync(queue_with_i15_1_plugin: TaskQueue):
    first_task = await queue_with_i15_1_plugin.get_task_by_position(0)
    assert first_task
    await queue_with_i15_1_plugin.move_task(first_task.id, 2)


def test__ensure_background_in_queue_or_tiled_returns_if_suitable_already_queued(
    i15_1_converter: I151Converter, background_not_found_in_tiled: None
):
    background = BackgroundInfo(bg_type="fq1.0", time_per_pdf=25)
    new_tasks = [make_background_task("fq1.0", 25)]
    result = i15_1_converter._ensure_background_in_queue_or_tiled(
        background, new_tasks, "task_id", "cm12345-1"
    )
    assert result == new_tasks


def test__ensure_background_in_queue_or_tiled_modifies_queued_background_if_possible(
    i15_1_converter: I151Converter, background_not_found_in_tiled: MagicMock
):
    background = BackgroundInfo(bg_type="fq1.0", time_per_pdf=25)
    new_tasks = [make_background_task("fq1.0", 10)]
    result = i15_1_converter._ensure_background_in_queue_or_tiled(
        background, new_tasks, "task_id", "cm12345-1"
    )
    assert len(result) == 1
    assert_tasks_equal(result[0], make_background_task("fq1.0", 25))


def test__ensure_background_in_queue_or_tiled_adds_background_if_none_suitable_in_queue(
    i15_1_converter: I151Converter,
    background_not_found_in_tiled: MagicMock,
):
    background = BackgroundInfo(bg_type="fq1.0", time_per_pdf=25)
    result = i15_1_converter._ensure_background_in_queue_or_tiled(
        background, [], "task_id", "cm12345-1"
    )
    assert len(result) == 1
    assert_tasks_equal(result[0], make_background_task("fq1.0", 25))


def test__ensure_background_in_queue_or_tiled_saves_tiled_info_if_exists(
    i15_1_converter: I151Converter,
    background_found_in_tiled: MagicMock,
):
    i15_1_converter._tiled_backgrounds["task_id"] = []
    background = BackgroundInfo(bg_type="fq1.0", time_per_pdf=25)
    result = i15_1_converter._ensure_background_in_queue_or_tiled(
        background, [], "task_id", "cm12345-1"
    )
    assert len(result) == 0
    assert i15_1_converter._tiled_backgrounds == {
        "task_id": [
            TiledBackground(bg_type="fq1.0", time_per_pdf=5, tiled_id="fake_tiled_id")
        ]
    }
