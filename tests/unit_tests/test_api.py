import uuid
from unittest.mock import MagicMock

import pytest
from blueapi.client import BlueapiClient
from blueapi.service.model import (
    TaskResponse,
)
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from daq_queuing_service.api.api import create_api_router
from daq_queuing_service.task import ExperimentDefinition, Status, TaskWithPosition
from daq_queuing_service.task_queue.queue import TaskQueue


@pytest.fixture
def test_client(task_queue_with_history: TaskQueue):
    blueapi_client = BlueapiClient(rest=MagicMock())
    blueapi_client.create_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )
    blueapi_client.clear_task = MagicMock(
        return_value=TaskResponse(task_id="blueapi_task_id")
    )

    app = FastAPI()
    app.include_router(
        create_api_router(task_queue_with_history, blueapi_client, MagicMock())
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


async def test_add_tasks_to_queue_adds_tasks_to_queue_and_returns_task_ids(
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

    assert last_task_after.id == second_last_task_before.id
    assert second_last_task_after.id == last_task_before.id
