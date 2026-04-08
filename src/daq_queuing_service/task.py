import time
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

type TaskID = str | UUID


class Status(StrEnum):
    WAITING = "Waiting"
    IN_PROGRESS = "In progress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


class ExperimentDefinition(BaseModel):
    # match ulims
    sample_id: str
    # experiment_id: str
    # something_unique: str  # then we wouldn't need task_id
    # params: dict


class Task(BaseModel):
    experiment_definition: ExperimentDefinition
    id: TaskID = Field(default_factory=uuid4)
    status: Status = Status.WAITING
    time_started: float | None = None
    time_completed: float | None = None

    def update_status(self, new_status: Status):
        self.status = new_status
        if new_status == Status.IN_PROGRESS:
            self.time_started = time.time()
        elif new_status == Status.COMPLETED:
            self.time_completed = time.time()
