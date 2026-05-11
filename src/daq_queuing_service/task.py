from collections.abc import Mapping
from typing import Any, Self
from uuid import uuid4

from blueapi.service.model import StrEnum
from pydantic import BaseModel, Field

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall, CallStatus


def create_uuid_str() -> str:
    return str(uuid4())


class Status(StrEnum):
    QUEUED = "Queued"
    IN_PROGRESS = "In progress"
    COMPLETE = "Complete"
    CANCELLED = "Cancelled"


class ExperimentDefinition(BaseModel):
    plan_name: str
    sample_id: str
    params: Mapping[str, Any] = Field(
        description="Values for parameters to plan, if any", default_factory=dict
    )
    instrument_session: str


class Task(BaseModel):
    experiment_definition: ExperimentDefinition
    id: str = Field(default_factory=create_uuid_str)
    blueapi_calls: list[BlueapiCall] = []
    _cancelled: bool = False

    def cancel(self):
        self._cancelled = True

    @property
    def status(self) -> Status:
        if self._cancelled:
            return Status.CANCELLED
        if self.blueapi_calls and all(
            call.status in [CallStatus.SUCCESS, CallStatus.ERROR]
            for call in self.blueapi_calls
        ):
            return Status.COMPLETE
        if any(
            call.status
            in [
                Status.IN_PROGRESS,
                CallStatus.CLAIMED,
                CallStatus.SUCCESS,
                CallStatus.ERROR,
            ]
            for call in self.blueapi_calls
        ):
            return Status.IN_PROGRESS
        return Status.QUEUED


class TaskWithPosition(Task):
    position: int | None

    @classmethod
    def from_task(cls, task: Task, position: int | None = None) -> Self:
        return cls.model_validate({**task.model_dump(), "position": position})
