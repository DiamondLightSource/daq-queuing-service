import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from blueapi.client.rest import (
    BlueapiRestClient,
    InvalidParametersError,
    ParameterError,
    UnknownPlanError,
)
from blueapi.service.model import (
    TaskResponse,
)
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from daq_queuing_service.api.api import (
    InvalidExperimentDefinitionsError,
    TaskCancelRequest,
    _validate_tasks_with_blueapi,
    create_api_router,
)
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.task import (
    ExperimentDefinition,
    Status,
    Task,
    TaskWithPosition,
)
from daq_queuing_service.task_queue.queue import TaskQueue
from daq_queuing_service.task_queue.queue_utils import QueueError

MOCK_TASK_REQUEST_CONSTRUCTOR = MagicMock()


@pytest.fixture
def blueapi_client():
    blueapi_client = BlueapiRestClient()
    blueapi_client.create_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )
    blueapi_client.clear_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )
    return blueapi_client


@pytest.fixture
def test_client(task_queue_with_history: TaskQueue, blueapi_client: BlueapiRestClient):
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(
        create_api_router(
            task_queue_with_history, blueapi_client, MOCK_TASK_REQUEST_CONSTRUCTOR
        )
    )
    return TestClient(app)


def test_read_root_returns_expected_string(test_client: TestClient):
    response = test_client.get("/")
    assert response.status_code == 200
    assert (
        response.json()
        == "Welcome to the daq queuing service. "
        + "Visit http://testserver/docs for Uvicorn API."
    )


def test_get_queue_state_returns_queue_state(test_client: TestClient):
    response = test_client.get("/queue/state")
    assert response.status_code == 200
    assert response.json() == {"paused": False}


def test_update_queue_state_changes_queue_state_and_returns_new_state(
    test_client: TestClient,
):
    response = test_client.patch("/queue/state", json={"paused": False})
    assert response.status_code == 200
    assert response.json() == {"paused": False}


def test_get_queued_tasks_returns_queued_task(test_client: TestClient):
    response = test_client.get("/queue")
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "2",
                "params": {},
                "instrument_session": "",
            },
            "id": "2",
            "status": "In progress",
            "time_started": "2026-04-17T15:02:00.000000",
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": "blueapi_id_2",
            "position": 0,
        },
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "3",
                "params": {},
                "instrument_session": "",
            },
            "id": "3",
            "status": "Waiting",
            "time_started": None,
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": None,
            "position": 1,
        },
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "4",
                "params": {},
                "instrument_session": "",
            },
            "id": "4",
            "status": "Waiting",
            "time_started": None,
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": None,
            "position": 2,
        },
    ]


def test_get_queued_tasks_can_filter_by_task_status(test_client: TestClient):
    response = test_client.get("/queue", params={"status": Status.IN_PROGRESS})
    assert response.status_code == 200
    print(response.json())
    assert response.json() == [
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "2",
                "params": {},
                "instrument_session": "",
            },
            "id": "2",
            "status": "In progress",
            "time_started": "2026-04-17T15:02:00.000000",
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": "blueapi_id_2",
            "position": 0,
        }
    ]


async def test_get_all_tasks_returns_all_tasks(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.get("/tasks")
    assert response.status_code == 200
    assert response.json() == jsonable_encoder(
        await task_queue_with_history.get_tasks()
    )


async def test_get_all_tasks_can_filter_by_task_status(test_client: TestClient):
    response = test_client.get("/tasks", params={"status": Status.SUCCESS})
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "1",
                "params": {},
                "instrument_session": "",
            },
            "id": "1",
            "status": "Success",
            "time_started": "2026-04-17T15:01:00.000000",
            "time_completed": "2026-04-17T15:01:59.000000",
            "errors": [],
            "result": {"outcome": "success", "result": None, "type": "NoneType"},
            "blueapi_id": "blueapi_id_1",
            "position": None,
        }
    ]


async def test_get_completed_tasks_returns_completed_tasks(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.get("/history")
    assert response.status_code == 200
    assert response.json() == jsonable_encoder(
        await task_queue_with_history.get_history()
    )


async def test_get_completed_tasks_can_filter_by_status(test_client: TestClient):

    response = test_client.get("/history", params={"status": Status.ERROR})
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "0",
                "params": {},
                "instrument_session": "",
            },
            "id": "0",
            "status": "Error",
            "time_started": "2026-04-17T15:00:00.000000",
            "time_completed": "2026-04-17T15:00:59.000000",
            "errors": [
                {
                    "outcome": "error",
                    "type": "ValueError",
                    "message": "Error during plan",
                }
            ],
            "result": None,
            "blueapi_id": "blueapi_id_0",
            "position": None,
        }
    ]


async def test_add_tasks_to_queue_validates_and_adds_to_queue_and_and_returns_task_ids(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.post(
        "/queue",
        json=[
            {
                "plan_name": "add_tasks",
                "sample_id": "1",
                "params": {"time": 10},
                "instrument_session": "abc",
            }
        ],
    )

    MOCK_TASK_REQUEST_CONSTRUCTOR.assert_called_once()

    assert response.status_code == 200
    task_ids: list[str] = response.json()
    [uuid.UUID(task_id) for task_id in task_ids]

    assert await task_queue_with_history.get_task_by_position(-1) == TaskWithPosition(
        experiment_definition=ExperimentDefinition(
            plan_name="add_tasks",
            sample_id="1",
            params={"time": 10},
            instrument_session="abc",
        ),
        id=task_ids[0],
        status=Status.WAITING,
        time_started=None,
        time_completed=None,
        errors=[],
        result=None,
        blueapi_id=None,
        position=3,
    )


@pytest.mark.parametrize(
    "payload, position, expected_status_code, expected_response_json",
    [
        [
            [
                {
                    "plan_name": "add_tasks",
                    "sample_id": "1",
                    "params": {"time": 10},
                    "instrument_session": "abc",
                }
            ],
            -1,
            400,
            {"error": "negative_position", "message": "Position must be >= 0, got -1"},
        ],
        [
            [
                {
                    "plan_name": "add_tasks",
                    "params": {"time": 10},
                    "instrument_session": "abc",
                }
            ],
            0,
            422,
            {
                "detail": [
                    {
                        "type": "missing",
                        "loc": ["body", 0, "sample_id"],
                        "msg": "Field required",
                        "input": {
                            "plan_name": "add_tasks",
                            "params": {"time": 10},
                            "instrument_session": "abc",
                        },
                    }
                ]
            },
        ],
    ],
)
def test_add_tasks_to_queue_with_bad_payload_gives_expected_error_responses(
    test_client: TestClient,
    payload: dict[str, Any],
    position: int,
    expected_status_code: int,
    expected_response_json: dict[str, Any],
):

    response = test_client.post("/queue", json=payload, params={"position": position})

    assert response.status_code == expected_status_code
    assert response.json() == expected_response_json


@pytest.mark.parametrize(
    "error, expected_response",
    [
        [
            InvalidExperimentDefinitionsError(
                {
                    0: InvalidParametersError(
                        errors=[
                            ParameterError(
                                loc=["bad_param"],
                                msg="fake_error",
                                type="extra_forbidden",
                                input="input",
                            )
                        ]
                    )
                }
            ),
            {
                "error": "invalid_experiment_definitions_error",
                "message": "Found validation errors for 1 experiment definitions. "
                + "No tasks have been added to the queue.",
                "details": {
                    "0": {
                        "type": "invalid_parameters",
                        "details": "Incorrect parameters supplied\n    "
                        + "Unexpected field 'bad_param'",
                    }
                },
            },
        ],
        [
            InvalidExperimentDefinitionsError({0: UnknownPlanError()}),
            {
                "error": "invalid_experiment_definitions_error",
                "message": "Found validation errors for 1 experiment definitions. "
                + "No tasks have been added to the queue.",
                "details": {"0": {"type": "unknown_plan", "details": ""}},
            },
        ],
    ],
)
def test_add_tasks_to_queue_if_tasks_fail_validation_then_expected_error_response_given(
    test_client: TestClient,
    error: InvalidExperimentDefinitionsError,
    expected_response: dict[str, Any],
):
    with patch(
        "daq_queuing_service.api.api._validate_tasks_with_blueapi", side_effect=error
    ):
        response = test_client.post(
            "/queue",
            json=[
                {
                    "plan_name": "add_tasks",
                    "sample_id": "1",
                    "params": {"time": 10},
                    "instrument_session": "abc",
                }
            ],
        )

    assert response.status_code == 409
    assert response.json() == expected_response


async def test_move_task_moves_task_and_returns_position(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    last_task_before = await task_queue_with_history.get_task_by_position(-1)
    second_last_task_before = await task_queue_with_history.get_task_by_position(-2)
    assert last_task_before
    assert second_last_task_before

    response = test_client.post(
        "/queue/move",
        params={
            "task_id": second_last_task_before.id,
            "new_position": last_task_before.position,
        },
    )

    assert response.json() == last_task_before.position

    last_task_after = await task_queue_with_history.get_task_by_position(-1)
    second_last_task_after = await task_queue_with_history.get_task_by_position(-2)
    assert last_task_after
    assert second_last_task_after

    assert response.status_code == 200
    assert last_task_after.id == second_last_task_before.id
    assert second_last_task_after.id == last_task_before.id


@pytest.mark.parametrize(
    "task_id, new_position, expected_status_code, expected_response_json",
    [
        [
            "bad_task_id",
            5,
            404,
            {
                "error": "task_not_found",
                "message": "'No task found matching id: bad_task_id'",
            },
        ],
        [
            "0",
            5,
            409,
            {"error": "task_not_in_queue", "message": "Task 0 isn't present in queue"},
        ],
        [
            "2",
            5,
            409,
            {
                "error": "task_in_progress",
                "message": "Cannot move task '2', it is currently in progress!",
            },
        ],
        [
            "3",
            -1,
            400,
            {"error": "negative_position", "message": "Position must be >= 0, got -1"},
        ],
    ],
)
def test_move_task_with_bad_params_gives_expected_error_responses(
    test_client: TestClient,
    task_id: str,
    new_position: int,
    expected_status_code: int,
    expected_response_json: dict[str, Any],
):

    response = test_client.post(
        "/queue/move",
        params={
            "task_id": task_id,
            "new_position": new_position,
        },
    )

    assert response.status_code == expected_status_code
    assert response.json() == expected_response_json


async def test_cancel_tasks_removes_task_from_queue_and_returns_tasks(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    queue = await task_queue_with_history.get_queue()
    last_two_task_ids = [task.id for task in queue[-2:]]

    response = test_client.request(
        "DELETE",
        "/queue/tasks",
        json=TaskCancelRequest(task_ids=last_two_task_ids).model_dump(),
    )

    queue_after = await task_queue_with_history.get_queue()
    task_ids_after = [task.id for task in queue_after]
    assert response.status_code == 200
    assert not any(task_id in task_ids_after for task_id in last_two_task_ids)
    assert response.json() == [
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "3",
                "params": {},
                "instrument_session": "",
            },
            "id": "3",
            "status": "Cancelled",
            "time_started": None,
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": None,
        },
        {
            "experiment_definition": {
                "plan_name": "test",
                "sample_id": "4",
                "params": {},
                "instrument_session": "",
            },
            "id": "4",
            "status": "Cancelled",
            "time_started": None,
            "time_completed": None,
            "errors": [],
            "result": None,
            "blueapi_id": None,
        },
    ]


@pytest.mark.parametrize(
    "payload, expected_status_code, expected_response_json",
    [
        [
            {"wrong": "field"},
            422,
            {
                "detail": [
                    {
                        "type": "missing",
                        "loc": ["body", "task_ids"],
                        "msg": "Field required",
                        "input": {"wrong": "field"},
                    }
                ]
            },
        ],
        [
            TaskCancelRequest(task_ids=["doesn't exist"]).model_dump(),
            404,
            {
                "error": "task_not_found",
                "message": '"No task found matching id: doesn\'t exist"',
            },
        ],
        [
            TaskCancelRequest(task_ids=["2", "3", "4"]).model_dump(),
            409,
            {
                "error": "task_in_progress",
                "message": "Cannot move task '2', it is currently in progress!",
            },
        ],
        [
            TaskCancelRequest(task_ids=["1", "2", "3", "4"]).model_dump(),
            409,
            {"error": "task_not_in_queue", "message": "Task 1 isn't present in queue"},
        ],
    ],
)
def test_cancel_tasks_with_bad_payloads_gives_expected_error_responses(
    test_client: TestClient,
    payload: str | dict[str, Any],
    expected_status_code: int,
    expected_response_json: dict[str, Any],
):

    response = test_client.request(
        "DELETE",
        "/queue/tasks",
        json=payload,
    )

    assert response.status_code == expected_status_code
    assert response.json() == expected_response_json


def test_get_task_by_position_returns_expected_task(test_client: TestClient):
    response = test_client.get("/queue/1")
    assert response.status_code == 200
    assert response.json() == {
        "experiment_definition": {
            "plan_name": "test",
            "sample_id": "3",
            "params": {},
            "instrument_session": "",
        },
        "id": "3",
        "status": "Waiting",
        "time_started": None,
        "time_completed": None,
        "errors": [],
        "result": None,
        "blueapi_id": None,
        "position": 1,
    }


def test_get_task_by_id_returns_expected_task(test_client: TestClient):
    response = test_client.get("/tasks/3")
    assert response.status_code == 200
    assert response.json() == {
        "experiment_definition": {
            "plan_name": "test",
            "sample_id": "3",
            "params": {},
            "instrument_session": "",
        },
        "id": "3",
        "status": "Waiting",
        "time_started": None,
        "time_completed": None,
        "errors": [],
        "result": None,
        "blueapi_id": None,
        "position": 1,
    }


def test_get_task_by_id_gives_error_if_task_id_does_not_exist(test_client: TestClient):
    response = test_client.get("/tasks/fake_id")
    assert response.status_code == 404
    assert response.json() == {
        "error": "task_not_found",
        "message": "'No task found matching id: fake_id'",
    }


async def test_clear_history_deletes_history(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    assert await task_queue_with_history.get_history()
    response = test_client.delete("/history")
    assert response.status_code == 200
    assert not await task_queue_with_history.get_history()


def test_queue_error_caught_by_error_handler(test_client: TestClient):
    with patch(
        "daq_queuing_service.api.api._validate_tasks_with_blueapi",
        side_effect=QueueError("Error in queue"),
    ):
        response = test_client.post(
            "/queue",
            json=[
                {
                    "plan_name": "add_tasks",
                    "sample_id": "1",
                    "params": {"time": 10},
                    "instrument_session": "abc",
                }
            ],
        )

    assert response.status_code == 409


def test__validate_tasks_with_blueapi_calls_create_task_and_then_removes_task(
    blueapi_client: BlueapiRestClient, tasks: list[Task]
):
    _validate_tasks_with_blueapi(tasks, blueapi_client, MagicMock())

    assert isinstance(blueapi_client.create_task, MagicMock)
    assert isinstance(blueapi_client.clear_task, MagicMock)
    assert blueapi_client.create_task.call_count == 5
    assert blueapi_client.clear_task.call_count == 5


def test__validate_tasks_with_blueapi_calls_collects_errors_and_raises(
    blueapi_client: BlueapiRestClient,
    tasks: list[Task],
):
    blueapi_client.create_task = MagicMock(side_effect=UnknownPlanError)

    with pytest.raises(InvalidExperimentDefinitionsError) as excinfo:
        _validate_tasks_with_blueapi(tasks, blueapi_client, MagicMock())

    error_raised = excinfo.value
    assert isinstance(error_raised, InvalidExperimentDefinitionsError)
    assert list(error_raised.errors.keys()) == [0, 1, 2, 3, 4]
    assert all(
        isinstance(error, UnknownPlanError) for error in error_raised.errors.values()
    )
