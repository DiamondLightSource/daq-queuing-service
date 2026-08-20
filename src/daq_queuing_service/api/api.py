import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Annotated

from blueapi.service.model import TaskRequest
from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import EventSourceResponse
from pydantic import BaseModel

from daq_queuing_service.app._config import AppConfig
from daq_queuing_service.app.authentication import User
from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCallResponse
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.converter import Converter, ValidateError
from daq_queuing_service.task_queue.queue import (
    QUEUE_EVENTS,
    PauseReason,
    QueueState,
    TaskQueue,
    TaskWithPosition,
)
from daq_queuing_service.task_queue.task import Experiment, Status, Task

# pyright: reportUnusedFunction=false


def get_current_user(request: Request) -> User | None:
    if hasattr(request.state, "user"):
        return request.state.user


CurrentUser = Annotated[User | None, Depends(get_current_user)]


class QueueStateUpdate(BaseModel):
    paused: bool


class TaskCancelRequest(BaseModel):
    task_ids: list[str]


def _filter_by_status(
    tasks: list[TaskWithPosition], status: Status | None
) -> list[TaskWithPosition]:
    if status is None:
        return tasks
    return [task for task in tasks if task.status == status]


def public_routes(queue: TaskQueue) -> APIRouter:
    """No authentication is required to access these endpoints."""
    router = APIRouter()

    @router.get("/")
    def read_root(request: Request):
        base_url = str(request.base_url)
        return (
            f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."
        )

    @router.get("/healthz")
    async def healthz():
        return Response()

    @router.get("/queue/state")
    def get_queue_state() -> QueueState:
        return queue.state

    return router


def protected_routes(
    queue: TaskQueue,
    broadcaster: Broadcaster[QUEUE_EVENTS],
    config: AppConfig,
    converter: Converter,
) -> APIRouter:
    """Authentication is required to access these endpoints (if turned on in config).
    Additionally, for endpoints that depend on whitelist_check, you must be in the
    whitelist of authorised fedIDs to access them.
    """
    router = APIRouter()

    @router.get("/config")
    def get_config() -> AppConfig:
        return config

    @router.patch("/queue/state")
    async def update_queue_state(payload: QueueStateUpdate) -> QueueState:
        if payload.paused:
            return await queue.pause_queue(PauseReason.USER_REQUESTED)
        else:
            return await queue.resume_queue()

    @router.get("/queue")
    async def get_queued_tasks(status: Status | None = None) -> list[TaskWithPosition]:
        return _filter_by_status(await queue.get_queue(), status)

    @router.post("/queue")
    async def add_tasks_to_queue(
        experiments: list[TaskRequest | Experiment],
        user: CurrentUser,
        position: int | None = None,
    ) -> list[str]:
        try:
            converter.validate(experiments)
        except Exception as e:
            raise ValidateError(*e.args) from e

        tasks = [Task(experiment=experiment, user=user) for experiment in experiments]
        task_ids = [task.id for task in tasks]
        await queue.add_tasks(tasks, position)
        return task_ids

    @router.delete("/queue")
    async def cancel_all_tasks() -> list[TaskWithPosition]:
        return await queue.cancel_all_tasks()

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
