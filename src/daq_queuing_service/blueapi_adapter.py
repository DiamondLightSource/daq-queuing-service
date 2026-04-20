import logging
from dataclasses import dataclass
from typing import Generic, TypeVar

from blueapi.client import BlueapiClient
from blueapi.client.event_bus import OnAnyEvent
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    UnknownPlanError,
)
from blueapi.service.model import TaskRequest
from blueapi.worker import TaskStatus, WorkerState

from daq_queuing_service.task import ExperimentDefinition

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


@dataclass(frozen=True)
class BlueapiResult(Generic[T, E]):
    value: T | None = None
    error: E | None = None

    def __post_init__(self):
        if (self.value is None) == (self.error is None):
            raise ValueError("Exactly one of value or error must be set")


class BlueapiClientAdapter:
    def __init__(
        self,
        client: BlueapiClient,
    ):
        self._client = client

    def get_state(self) -> BlueapiResult[WorkerState, ServiceUnavailableError]:
        try:
            return BlueapiResult(value=self._client.get_state())
        except ServiceUnavailableError as e:
            LOGGER.error(f"Lost connection to blueapi: {e}")
            return BlueapiResult(error=e)

    def run_task(
        self,
        task_request: TaskRequest,
        on_event: OnAnyEvent | None = None,
    ) -> BlueapiResult[
        TaskStatus,
        BlueskyRemoteControlError
        | InvalidParametersError
        | UnknownPlanError
        | ServiceUnavailableError,
    ]:
        try:
            return BlueapiResult(self._client.run_task(task_request, on_event=on_event))
        except (
            BlueskyRemoteControlError,
            InvalidParametersError,
            UnknownPlanError,
            ServiceUnavailableError,
        ) as e:
            LOGGER.exception(e)
            return BlueapiResult(error=e)
        except Exception as e:
            LOGGER.exception(f"Exception: {e}")
            raise e


def construct_blueapi_task_request(
    experiment_definition: ExperimentDefinition,
) -> TaskRequest:
    return TaskRequest(
        name=experiment_definition.plan_name,
        params=experiment_definition.params,
        instrument_session=experiment_definition.instrument_session,
    )
