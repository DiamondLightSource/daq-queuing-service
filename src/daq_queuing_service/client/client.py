from collections.abc import Mapping
from typing import Any, TypeVar

import requests
from pydantic import HttpUrl, TypeAdapter

from daq_queuing_service.api.api import TaskCancelRequest
from daq_queuing_service.app._config import AppConfig
from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCallResponse
from daq_queuing_service.task import ExperimentDefinition, TaskWithPosition
from daq_queuing_service.task_queue.queue import QueueState

T = TypeVar("T")


class QueueClient:
    def __init__(self, url: str):
        self._url = HttpUrl(url)
        self._pool = requests.Session()

    def _request(
        self,
        suffix: str,
        target_type: type[T],
        method: str = "GET",
        data: Any = None,
        params: Mapping[str, Any] | None = None,
    ):
        url = self._url.unicode_string().removesuffix("/") + suffix
        response = self._pool.request(
            method,
            url,
            json=data,
            params=params,
        )

        return TypeAdapter(target_type).validate_python(response.json())

    def healthz(self):
        return self._request("/healthz", str)

    def get_config(self):
        return self._request("/config", AppConfig)

    def get_queue_state(self) -> QueueState:
        return self._request("/queue/state", QueueState)

    def update_queue_state(self, new_state: QueueState) -> QueueState:
        return self._request(
            "/queue/state", QueueState, method="PATCH", data=new_state.model_dump()
        )

    def get_queued_tasks(self) -> list[TaskWithPosition]:
        return self._request("/queue", list[TaskWithPosition])

    def add_tasks_to_queue(
        self,
        experiment_definitions: list[ExperimentDefinition],
        position: int | None = None,
        validate_with_blueapi: bool = True,
    ) -> list[str]:
        return self._request(
            "/queue",
            list[str],
            method="POST",
            data=[exp_def.model_dump() for exp_def in experiment_definitions],
            params={
                "position": position,
                "validate_with_blueapi": validate_with_blueapi,
            },
        )

    def move_task(self, task_id: str, new_position: int):
        return self._request(
            "/queue/move",
            int,
            method="POST",
            params={"task_id": task_id, "new_position": new_position},
        )

    def cancel_tasks(self, task_ids: list[str]) -> list[TaskWithPosition]:
        return self._request(
            "/queue/tasks",
            list[TaskWithPosition],
            method="DELETE",
            data=TaskCancelRequest(task_ids=task_ids).model_dump(),
        )

    def cancel_all_tasks(self) -> list[TaskWithPosition]:
        return self.cancel_all_tasks()

    def get_task_by_position(self, position: int) -> TaskWithPosition:
        return self._request(f"/queue/{position}", TaskWithPosition)

    def get_all_tasks(self) -> list[TaskWithPosition]:
        return self._request("/tasks", list[TaskWithPosition])

    def get_task_by_id(self, task_id: str) -> TaskWithPosition:
        return self._request(f"/tasks/{task_id}", TaskWithPosition)

    def get_completed_tasks(self) -> list[TaskWithPosition]:
        return self._request("/history", list[TaskWithPosition])

    def clear_history(self):
        return self._request("/history", str, "DELETE")

    def get_call_queue(self) -> list[BlueapiCallResponse]:
        return self._request("/call_queue", list[BlueapiCallResponse])

    def get_call_history(self) -> list[BlueapiCallResponse]:
        return self._request("/call_history", list[BlueapiCallResponse])
