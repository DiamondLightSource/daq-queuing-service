import logging
from collections.abc import Callable

from blueapi.client import BlueapiClient
from blueapi.client.rest import InvalidParametersError, UnknownPlanError
from blueapi.service.model import TaskRequest
from fastapi import APIRouter, Request
from pydantic import BaseModel

from daq_queuing_service.task import ExperimentDefinition, Status, Task
from daq_queuing_service.task_queue.queue import (
    QueueState,
    TaskQueue,
    TaskWithPosition,
)

LOGGER = logging.getLogger(__name__)

# pyright: reportUnusedFunction=false


class InvalidExperimentDefinitionsError(Exception):
    def __init__(self, errors: dict[int, InvalidParametersError | UnknownPlanError]):
        self.errors = errors

    pass


class QueueStateUpdate(BaseModel):
    paused: bool | None = None


class TaskCancelRequest(BaseModel):
    task_ids: list[str]


def _filter_by_status(
    tasks: list[TaskWithPosition], status: Status | None
) -> list[TaskWithPosition]:
    if status is None:
        return tasks
    return [task for task in tasks if task.status == status]


def _validate_tasks_with_blueapi(
    tasks: list[Task],
    blueapi_client: BlueapiClient,
    task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
) -> None:
    errors: dict[int, InvalidParametersError | UnknownPlanError] = {}
    for i, task in enumerate(tasks):
        try:
            task_response = blueapi_client.create_task(
                task_request_constructor(task.experiment_definition)
            )
            blueapi_client.clear_task(task_response.task_id)
        except (InvalidParametersError, UnknownPlanError) as e:
            errors[i] = e
    if errors:
        raise InvalidExperimentDefinitionsError(errors)


def create_api_router(
    queue: TaskQueue,
    blueapi_client: BlueapiClient,
    task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
) -> APIRouter:
    router = APIRouter()

    @router.get("/")
    def read_root(request: Request):
        base_url = str(request.base_url)
        return (
            f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."
        )

    @router.patch("/queue/state")
    async def update_queue_state(payload: QueueStateUpdate) -> QueueState:
        return await queue.update_state(**payload.model_dump(exclude_none=True))

    @router.get("/queue/state")
    def get_queue_state() -> QueueState:
        return queue.state

    @router.get("/queue")
    async def get_queued_tasks(status: Status | None = None) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_queue(), status)

    @router.post("/queue")
    async def add_tasks_to_queue(
        experiment_definitions: list[ExperimentDefinition], position: int | None = None
    ) -> list[str]:
        tasks = [
            Task(experiment_definition=experiment_definition)
            for experiment_definition in experiment_definitions
        ]
        _validate_tasks_with_blueapi(tasks, blueapi_client, task_request_constructor)
        task_ids = [task.id for task in tasks]
        await queue.add_tasks(tasks, position)
        return task_ids

    @router.post("/queue/move")
    async def move_task(task_id: str, new_position: int) -> int:
        return await queue.move_task(task_id, new_position)

    @router.delete("/queue/tasks")
    async def cancel_tasks(payload: TaskCancelRequest) -> list[Task]:
        return await queue.cancel_tasks(payload.task_ids)

    @router.get("/queue/{position}")
    async def get_task_by_position(position: int) -> TaskWithPosition | None:
        return await queue.get_task_by_position(position)

    @router.get("/tasks")
    async def get_all_tasks(status: Status | None = None) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_tasks(), status)

    @router.get("/tasks/{task_id}")
    async def get_task_by_id(task_id: str) -> TaskWithPosition:
        return await queue.get_task_by_id(task_id)

    @router.delete("/history")
    async def clear_history():
        return await queue.clear_history()

    @router.get("/history")
    async def get_historic_tasks(
        status: Status | None = None,
    ) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_history(), status)

    return router
