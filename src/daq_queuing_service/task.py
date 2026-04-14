import time
from enum import StrEnum
from typing import Self
from uuid import uuid4

from pydantic import BaseModel, Field


def _create_uuid_str() -> str:
    return str(uuid4())


class Status(StrEnum):
    WAITING = "Waiting"  # Waiting in the queue
    CLAIMED = "Claimed"  # Claimed by the worker
    IN_PROGRESS = "In progress"  # In progress inside BlueAPI
    SUCCESS = "Success"  # Completed successfully
    ERROR = "Error"  # Error while trying to run
    CANCELLED = "Cancelled"  # Cancelled before being run

    @property
    def allowed_transitions(self):
        allowed_transitions: dict[Status, set[Status]] = {  # from: to
            Status.WAITING: {Status.CLAIMED, Status.CANCELLED},
            Status.CLAIMED: {Status.WAITING, Status.IN_PROGRESS, Status.ERROR},
            Status.IN_PROGRESS: {Status.SUCCESS, Status.ERROR},
            Status.SUCCESS: set(),
            Status.ERROR: set(),
            Status.CANCELLED: set(),
        }
        return allowed_transitions[self]


class ExperimentDefinition(BaseModel):
    # match ulims
    sample_id: str
    # experiment_id: str
    # something_unique: str  # then we wouldn't need task_id
    # params: dict


class Task(BaseModel):
    experiment_definition: ExperimentDefinition
    id: str = Field(default_factory=_create_uuid_str)
    status: Status = Status.WAITING
    time_started: float | None = None
    time_completed: float | None = None
    errors: list[str] = Field(default_factory=list[str])
    blueapi_id: str | None = None

    def _update_status(self, new_state: Status):
        allowed = self.status.allowed_transitions
        if new_state not in allowed:
            raise ValueError(
                f"Can't go from current state '{self.status}' to '{new_state}'. "
                + f"Allowed transitions from {self.status}: {allowed}."
            )
        self.status = new_state

    def wait(self):
        self._update_status(Status.WAITING)

    def claim(self):
        self._update_status(Status.CLAIMED)

    def put_in_progress(self, blueapi_id: str):
        self._update_status(Status.IN_PROGRESS)
        self.time_started = time.time()
        self.blueapi_id = blueapi_id

    def succeed(self):
        self._update_status(Status.SUCCESS)
        self.time_completed = time.time()

    def fail(self, errors: list[str] | None = None):
        self._update_status(Status.ERROR)
        if errors:
            self.errors.extend(errors)
        if self.time_started:
            self.time_completed = time.time()

    def cancel(self):
        self._update_status(Status.CANCELLED)


class TaskWithPosition(Task):
    position: int | None

    @classmethod
    def from_task(cls, task: Task, position: int | None = None) -> Self:
        return cls.model_validate({**task.model_dump(), "position": position})
