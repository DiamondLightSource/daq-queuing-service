from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4

from blueapi.service.model import TaskRequest
from pydantic import BaseModel, Field, computed_field

from daq_queuing_service.blueapi_interaction.blueapi_call import (
    BlueapiCall,
    BlueapiCallResponse,
    CallStatus,
)


class Sample(BaseModel):
    name: str
    id: str
    data: Mapping[str, Any]


class ExperimentDefinition(BaseModel):
    name: str
    id: str
    data: Mapping[str, Any]


class Experiment(BaseModel):
    instrument_session: str
    sample: Sample
    experiment_definition: ExperimentDefinition


def create_uuid_str() -> str:
    return str(uuid4())


class Status(StrEnum):
    QUEUED = "Queued"
    IN_PROGRESS = "In progress"
    COMPLETE = "Complete"
    CANCELLED = "Cancelled"


class TaskKind(StrEnum):
    EXPERIMENT = "Experiment"
    PLAN = "Plan"


class Task(BaseModel):
    experiment: Experiment | TaskRequest
    id: str = Field(default_factory=create_uuid_str)
    blueapi_calls: list[BlueapiCall] = Field(default_factory=lambda: [])
    _cancelled: bool = False

    def cancel(self):
        self._cancelled = True

    @computed_field
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

    @computed_field
    @property
    def kind(self) -> TaskKind:
        match self.experiment:
            case Experiment():
                return TaskKind.EXPERIMENT
            case TaskRequest():
                return TaskKind.PLAN


class TaskWithPosition(BaseModel):
    experiment: Experiment | TaskRequest
    id: str
    status: Status
    blueapi_calls: list[BlueapiCallResponse]
    position: int | None
    kind: TaskKind

    @classmethod
    def from_task(cls, task: Task, position: int | None = None) -> Self:
        return cls.model_validate({**task.model_dump(), "position": position})
