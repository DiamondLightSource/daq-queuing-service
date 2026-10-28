from collections.abc import Mapping
from typing import Any, TypeVar

import requests
from blueapi.service.authentication import JWTAuth
from blueapi.service.model import TaskRequest
from pydantic import TypeAdapter

from daq_queuing_service.external_interaction.blueapi.token_source import UDCTokenSource
from daq_queuing_service.task_queue.task import Experiment

T = TypeVar("T")


# This should live in the runtime estimator repo, but how would we import it?
# It is not on pypi, and is private on gitlab
class RuntimeEstimatorClient:
    def __init__(self, url: str, token_source: UDCTokenSource):
        self.url = url
        self.token_source = token_source
        self._pool = requests.Session()

    def _request_and_deserialize(
        self,
        suffix: str,
        target_type: type[T],
        data: Any,
        method: str = "POST",
        params: Mapping[str, Any] | None = None,
    ) -> T:
        url = self.url.removesuffix("/") + suffix

        response = self._pool.request(
            method,
            url,
            json=data,
            params=params,
            auth=JWTAuth(self.token_source),  # type: ignore
        )

        deserialized = TypeAdapter(target_type).validate_python(response.json())
        return deserialized

    def estimate_runtime(self, tasks: list[Experiment | TaskRequest], instrument: str):
        return self._request_and_deserialize(
            f"/estimate-runtime/{instrument}",
            int,
            [task.model_dump(mode="json") for task in tasks],
        )

    def estimate_runtime_per_task(
        self, tasks: list[Experiment | TaskRequest], instrument: str
    ):
        return self._request_and_deserialize(
            f"/estimate-runtime-per-task/{instrument}",
            list[int],
            [task.model_dump(mode="json") for task in tasks],
        )
