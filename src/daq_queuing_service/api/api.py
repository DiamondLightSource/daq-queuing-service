import asyncio
import json
from collections.abc import AsyncGenerator, Callable

from blueapi.client.rest import (
    BlueapiRestClient,
    InvalidParametersError,
    UnknownPlanError,
)
from blueapi.service.model import TaskRequest
from fastapi import APIRouter, Request, Response
from fastapi.responses import EventSourceResponse
from pydantic import BaseModel

from daq_queuing_service.app._config import AppConfig, load_config
from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCallResponse
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.log import LOGGER
from daq_queuing_service.task import ExperimentDefinition, Status, Task
from daq_queuing_service.task_queue.queue import (
    QUEUE_EVENTS,
    QueueState,
    TaskQueue,
    TaskWithPosition,
)

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
    blueapi_client: BlueapiRestClient,
    task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
) -> None:
    errors: dict[int, InvalidParametersError | UnknownPlanError] = {}
    LOGGER.info(f"Using blueapi client: {blueapi_client._config}")  # type: ignore # noqa
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
    blueapi_client: BlueapiRestClient,
    task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
    broadcaster: Broadcaster[QUEUE_EVENTS],
) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz():
        return Response()

    @router.get("/")
    def read_root(request: Request):
        base_url = str(request.base_url)
        return (
            f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."
        )

    @router.get("/config")
    def get_config() -> AppConfig:
        return load_config()

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
        experiment_definitions: list[ExperimentDefinition],
        position: int | None = None,
        validate_with_blueapi: bool = True,
    ) -> list[str]:
        tasks = [
            Task(experiment_definition=experiment_definition)
            for experiment_definition in experiment_definitions
        ]
        if validate_with_blueapi:
            _validate_tasks_with_blueapi(
                tasks, blueapi_client, task_request_constructor
            )
        task_ids = [task.id for task in tasks]
        await queue.add_tasks(tasks, position)
        return task_ids

    @router.post("/queue/move")
    async def move_task(task_id: str, new_position: int) -> int:
        return await queue.move_task(task_id, new_position)

    @router.delete("/queue/tasks")
    async def cancel_tasks(payload: TaskCancelRequest) -> list[TaskWithPosition]:
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

    @router.get("/history")
    async def get_completed_tasks(
        status: Status | None = None,
    ) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_history(), status)

    @router.delete("/history")
    async def clear_history():
        return await queue.clear_history()

    @router.get("/call_queue")
    async def get_call_queue() -> list[BlueapiCallResponse]:
        return await queue.get_call_queue()

    @router.get("/call_history")
    async def get_call_history() -> list[BlueapiCallResponse]:
        return await queue.get_call_history()

    @router.get("/events")
    async def stream_events() -> EventSourceResponse:
        subscriber = broadcaster.subscribe()

        async def event_generator() -> AsyncGenerator[str, None]:
            try:
                while True:
                    event = await subscriber.get()
                    event_str = (
                        f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                    )
                    yield event_str

            except asyncio.CancelledError:
                # Client disconnected
                raise
            finally:
                broadcaster.unsubscribe(subscriber)

        return EventSourceResponse(event_generator())

    return router
