from collections.abc import Mapping
from typing import Any, TypeVar

import requests
from pydantic import HttpUrl, TypeAdapter

from daq_queuing_service.api.api import TaskCancelRequest
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
    ) -> list[str]:
        return self._request(
            "/queue",
            list[str],
            method="POST",
            data=[exp_def.model_dump() for exp_def in experiment_definitions],
            params={"position": position},
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
        task_ids = [task.id for task in self.get_queued_tasks()]
        return self.cancel_tasks(task_ids)
