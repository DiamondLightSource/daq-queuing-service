import threading
import time
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from blueapi.client.rest import (
    BlueapiRestClient,
)
from blueapi.service.model import (
    TaskRequest,
    TaskResponse,
)
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from daq_queuing_service.api.api import (
    TaskCancelRequest,
    create_api_router,
)
from daq_queuing_service.api.errors import register_exception_handlers
from daq_queuing_service.app._config import TEST_CONFIG_PATH, load_config
from daq_queuing_service.blueapi_interaction.blueapi_call import (
    BlueapiCallResponse,
    CallStatus,
)
from daq_queuing_service.broadcaster import Broadcaster, Event
from daq_queuing_service.task import (
    Status,
    TaskKind,
    TaskWithPosition,
)
from daq_queuing_service.task_queue.queue import QUEUE_EVENTS, PauseReason, TaskQueue
from daq_queuing_service.task_queue.queue_utils import QueueError


@pytest.fixture
def blueapi_client() -> BlueapiRestClient:
    blueapi_client = BlueapiRestClient()
    blueapi_client.create_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )
    blueapi_client.clear_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )
    return blueapi_client


@pytest.fixture
def broadcaster() -> Broadcaster[QUEUE_EVENTS]:
    return Broadcaster()


@pytest.fixture
def app(
    task_queue_with_history: TaskQueue,
    blueapi_client: BlueapiRestClient,
    broadcaster: Broadcaster[QUEUE_EVENTS],
) -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(
        create_api_router(
            task_queue_with_history,
            broadcaster,
            load_config(Path(TEST_CONFIG_PATH)),
        )
    )
    return app


@pytest.fixture
def test_client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_read_root_returns_expected_string(test_client: TestClient):
    response = test_client.get("/")
    assert response.status_code == 200
    assert (
        response.json()
        == "Welcome to the daq queuing service. "
        + "Visit http://testserver/docs for Uvicorn API."
    )


def test_healthz_returns_healthy(test_client: TestClient):
    response = test_client.get("/healthz")
    assert response.status_code == 200


def test_get_queue_state_returns_queue_state(test_client: TestClient):
    response = test_client.get("/queue/state")
    assert response.status_code == 200
    assert response.json() == {
        "paused": False,
        "last_pause_reason": "Paused as queue completed",
    }


def test_resume_changes_queue_state_and_returns_new_state(
    test_client: TestClient,
):
    response = test_client.patch("/queue/state", json={"paused": False})
    assert response.status_code == 200
    assert response.json() == {
        "paused": False,
        "last_pause_reason": PauseReason.EMPTY_QUEUE,
    }


def test_pause_changes_queue_state_and_returns_new_state(
    test_client: TestClient,
):
    response = test_client.patch("/queue/state", json={"paused": True})
    assert response.status_code == 200
    assert response.json() == {
        "paused": True,
        "last_pause_reason": PauseReason.USER_REQUESTED,
    }


def test_get_queued_tasks_returns_queued_task(test_client: TestClient):
    response = test_client.get("/queue")
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_2", "id": "2", "data": {}},
                "experiment_definition": {"name": "test", "id": "2", "data": {}},
            },
            "id": "2",
            "status": "In progress",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "2",
                    "status": "In progress",
                    "time_started": "2026-04-17T15:02:00.000000",
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": 0,
            "kind": "Experiment",
        },
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_3", "id": "3", "data": {}},
                "experiment_definition": {"name": "test", "id": "3", "data": {}},
            },
            "id": "3",
            "status": "Queued",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "3",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": 1,
            "kind": "Experiment",
        },
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_4", "id": "4", "data": {}},
                "experiment_definition": {"name": "test", "id": "4", "data": {}},
            },
            "id": "4",
            "status": "Queued",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "4",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": 2,
            "kind": "Experiment",
        },
    ]


def test_get_queued_tasks_can_filter_by_task_status(test_client: TestClient):
    response = test_client.get("/queue", params={"status": Status.IN_PROGRESS})
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_2", "id": "2", "data": {}},
                "experiment_definition": {"name": "test", "id": "2", "data": {}},
            },
            "id": "2",
            "status": "In progress",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "2",
                    "status": "In progress",
                    "time_started": "2026-04-17T15:02:00.000000",
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": 0,
            "kind": "Experiment",
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
    response = test_client.get("/tasks", params={"status": Status.COMPLETE})
    assert response.status_code == 200
    assert response.json() == [
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_1", "id": "1", "data": {}},
                "experiment_definition": {"name": "test", "id": "1", "data": {}},
            },
            "id": "1",
            "status": "Complete",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "1",
                    "status": "Success",
                    "time_started": "2026-04-17T15:01:00.000000",
                    "time_completed": "2026-04-17T15:01:59.000000",
                    "result": {
                        "outcome": "success",
                        "result": None,
                        "type": "NoneType",
                    },
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": None,
            "kind": "Experiment",
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


async def test_add_tasks_to_queue_adds_to_queue_and_and_returns_task_ids(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.post(
        "/queue",
        json=[
            {
                "name": "add_tasks",
                "params": {"time": 10},
                "instrument_session": "abc",
            }
        ],
    )

    assert response.status_code == 200
    task_ids: list[str] = response.json()
    [uuid.UUID(task_id) for task_id in task_ids]

    assert await task_queue_with_history.get_task_by_position(-1) == TaskWithPosition(
        experiment=TaskRequest(
            name="add_tasks",
            params={"time": 10},
            instrument_session="abc",
        ),
        id=task_ids[-1],
        blueapi_calls=[
            BlueapiCallResponse(
                task_request=TaskRequest(
                    name="add_tasks", params={"time": 10}, instrument_session="abc"
                ),
                parent_task_id=task_ids[-1],
                status=CallStatus.WAITING,
                time_started=None,
                time_completed=None,
                result=None,
                errors=[],
                blueapi_id=None,
            )
        ],
        position=3,
        status=Status.QUEUED,
        kind=TaskKind.PLAN,
    )


@pytest.mark.parametrize(
    "payload, position, expected_status_code, expected_response_json",
    [
        [
            [
                {
                    "name": "test",
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
                    "name": "test",
                    "params": {"time": 10},
                }
            ],
            0,
            422,
            {
                "detail": [
                    {
                        "type": "missing",
                        "loc": ["body", 0, "TaskRequest", "instrument_session"],
                        "msg": "Field required",
                        "input": {"name": "test", "params": {"time": 10}},
                    },
                    {
                        "type": "missing",
                        "loc": ["body", 0, "Experiment", "instrument_session"],
                        "msg": "Field required",
                        "input": {"name": "test", "params": {"time": 10}},
                    },
                    {
                        "type": "missing",
                        "loc": ["body", 0, "Experiment", "sample"],
                        "msg": "Field required",
                        "input": {"name": "test", "params": {"time": 10}},
                    },
                    {
                        "type": "missing",
                        "loc": ["body", 0, "Experiment", "experiment_definition"],
                        "msg": "Field required",
                        "input": {"name": "test", "params": {"time": 10}},
                    },
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
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_3", "id": "3", "data": {}},
                "experiment_definition": {"name": "test", "id": "3", "data": {}},
            },
            "id": "3",
            "status": "Cancelled",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "3",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": None,
            "kind": "Experiment",
        },
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_4", "id": "4", "data": {}},
                "experiment_definition": {"name": "test", "id": "4", "data": {}},
            },
            "id": "4",
            "status": "Cancelled",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "4",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": None,
            "kind": "Experiment",
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


async def test_cancel_all_tasks_removes_all_queued_tasks_from_queue_and_returns_them(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    queue = await task_queue_with_history.get_queue()
    should_be_cancelled = [task for task in queue if task.status == Status.QUEUED]
    response = test_client.request("DELETE", "/queue")

    queue_after = await task_queue_with_history.get_queue()
    task_ids_after = [task.id for task in queue_after]
    assert response.status_code == 200
    assert not any(task_id in task_ids_after for task_id in should_be_cancelled)
    assert response.json() == [
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_3", "id": "3", "data": {}},
                "experiment_definition": {"name": "test", "id": "3", "data": {}},
            },
            "id": "3",
            "status": "Cancelled",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "3",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": None,
            "kind": "Experiment",
        },
        {
            "experiment": {
                "name": "test_experiment",
                "instrument_session": "",
                "sample": {"name": "test_8_4", "id": "4", "data": {}},
                "experiment_definition": {"name": "test", "id": "4", "data": {}},
            },
            "id": "4",
            "status": "Cancelled",
            "blueapi_calls": [
                {
                    "task_request": {
                        "name": "test",
                        "params": {},
                        "instrument_session": "",
                    },
                    "parent_task_id": "4",
                    "status": "Waiting",
                    "time_started": None,
                    "time_completed": None,
                    "result": None,
                    "errors": [],
                    "blueapi_id": None,
                }
            ],
            "position": None,
            "kind": "Experiment",
        },
    ]


def test_get_task_by_position_returns_expected_task(test_client: TestClient):
    response = test_client.get("/queue/1")
    assert response.status_code == 200
    assert response.json() == {
        "experiment": {
            "name": "test_experiment",
            "instrument_session": "",
            "sample": {"name": "test_8_3", "id": "3", "data": {}},
            "experiment_definition": {"name": "test", "id": "3", "data": {}},
        },
        "id": "3",
        "status": "Queued",
        "blueapi_calls": [
            {
                "task_request": {
                    "name": "test",
                    "params": {},
                    "instrument_session": "",
                },
                "parent_task_id": "3",
                "status": "Waiting",
                "time_started": None,
                "time_completed": None,
                "result": None,
                "errors": [],
                "blueapi_id": None,
            }
        ],
        "position": 1,
        "kind": "Experiment",
    }


def test_get_task_by_id_returns_expected_task(test_client: TestClient):
    response = test_client.get("/tasks/3")
    assert response.status_code == 200
    assert response.json() == {
        "experiment": {
            "name": "test_experiment",
            "instrument_session": "",
            "sample": {"name": "test_8_3", "id": "3", "data": {}},
            "experiment_definition": {"name": "test", "id": "3", "data": {}},
        },
        "id": "3",
        "status": "Queued",
        "blueapi_calls": [
            {
                "task_request": {
                    "name": "test",
                    "params": {},
                    "instrument_session": "",
                },
                "parent_task_id": "3",
                "status": "Waiting",
                "time_started": None,
                "time_completed": None,
                "result": None,
                "errors": [],
                "blueapi_id": None,
            }
        ],
        "position": 1,
        "kind": "Experiment",
    }


def test_get_task_by_id_gives_error_if_task_id_does_not_exist(test_client: TestClient):
    response = test_client.get("/tasks/fake_id")
    assert response.status_code == 404
    assert response.json() == {
        "error": "task_not_found",
        "message": "'No task found matching id: fake_id'",
    }


def test_get_call_queue_returns_calls_in_call_queue(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.get("/call_queue")
    assert response.status_code == 200
    assert response.json() == jsonable_encoder(task_queue_with_history._call_queue)


def test_get_call_history_returns_calls_in_call_queue(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    response = test_client.get("/call_history")
    assert response.status_code == 200
    assert response.json() == jsonable_encoder(task_queue_with_history._call_history)


async def test_clear_history_deletes_history(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    assert await task_queue_with_history.get_history()
    response = test_client.delete("/history")
    assert response.status_code == 200
    assert not await task_queue_with_history.get_history()


def test_any_queue_error_caught_by_error_handler(
    test_client: TestClient, task_queue_with_history: TaskQueue
):
    class SomeError(QueueError): ...

    with patch.object(
        task_queue_with_history, "add_tasks", side_effect=SomeError("Error in queue")
    ):
        response = test_client.post(
            "/queue",
            json=[
                {
                    "name": "add_tasks",
                    "params": {"time": 10},
                    "instrument_session": "abc",
                }
            ],
        )

    assert response.status_code == 409


def test_get_config_returns_config(test_client: TestClient):
    response = test_client.get("/config")
    assert response.status_code == 200
    assert response.json()["converter"] == {
        "path": "daq_queuing_service.plugins.converter",
        "name": "Converter",
    }


@pytest.mark.skip("Can't get TestClient to play nicely with SSE")
async def test_stream_events_streams_all_events_from_broadcaster(
    test_client: TestClient, broadcaster: Broadcaster[QUEUE_EVENTS]
):
    events_to_send: list[Event[QUEUE_EVENTS]] = [
        Event(type="queue_update", data=i) for i in range(5)
    ]
    received: list[str] = []

    def read_stream():
        with test_client.stream("GET", "/events") as response:
            for line in response.iter_raw():
                # Never getting response
                if not line:
                    continue

                received.append(line.decode())
                if len(received) == 5:
                    break

    thread = threading.Thread(target=read_stream)
    thread.start()
    time.sleep(0.1)
    for event in events_to_send:
        broadcaster.broadcast(event)

    time.sleep(0.2)
    assert len(received) == 5
    assert received != []
