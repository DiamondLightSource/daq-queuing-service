import asyncio
import json
from collections.abc import AsyncGenerator

from blueapi.service.model import TaskRequest
from fastapi import APIRouter, Request, Response
from fastapi.responses import EventSourceResponse
from pydantic import BaseModel

from daq_queuing_service.app._config import AppConfig
from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCallResponse
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.task import Experiment, Status, Task
from daq_queuing_service.task_queue.queue import (
    QUEUE_EVENTS,
    QueueState,
    TaskQueue,
    TaskWithPosition,
)

# pyright: reportUnusedFunction=false


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


def create_api_router(
    queue: TaskQueue,
    broadcaster: Broadcaster[QUEUE_EVENTS],
    config: AppConfig,
) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> Response:
        return Response()

    @router.get("/")
    def read_root(request: Request) -> str:
        base_url = str(request.base_url)
        return (
            f"Welcome to the daq queuing service. Visit {base_url}docs for Uvicorn API."
        )

    @router.get("/config")
    def get_config() -> AppConfig:
        return config

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
        experiments: list[TaskRequest | Experiment],
        position: int | None = None,
    ) -> list[str]:
        tasks = [Task(experiment=experiment) for experiment in experiments]
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
