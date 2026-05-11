from datetime import datetime
from enum import StrEnum

from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskError, TaskResult
from pydantic import BaseModel, Field


class Status(StrEnum):
    WAITING = "Waiting"  # Waiting in the queue
    CLAIMED = "Claimed"  # Claimed by the worker
    IN_PROGRESS = "In progress"  # In progress inside BlueAPI
    SUCCESS = "Success"  # Completed successfully
    ERROR = "Error"  # Error while trying to run

    @property
    def allowed_transitions(self):
        allowed_transitions: dict[Status, set[Status]] = {  # from: to
            Status.WAITING: {Status.CLAIMED},
            Status.CLAIMED: {Status.WAITING, Status.IN_PROGRESS, Status.ERROR},
            Status.IN_PROGRESS: {Status.SUCCESS, Status.ERROR},
            Status.SUCCESS: set(),
            Status.ERROR: set(),
        }
        return allowed_transitions[self]


class BlueapiCall(BaseModel):
    task_request: TaskRequest
    parent_task_id: str | None = None
    status: Status = Status.WAITING
    time_started: str | None = None
    time_completed: str | None = None
    result: TaskResult | None = None
    errors: list[str | TaskError] = Field(default_factory=list[str | TaskError])
    blueapi_id: str | None = None

    def _update_status(self, new_status: Status):
        """Updates the status of the task, checking that the transition is valid

        Args:
            new_status (Status): New status of the task

        Raises:
            ValueError: Raised if the transition from the task's current status to the
            new status is not permitted.
        """
        allowed = self.status.allowed_transitions
        if new_status not in allowed:
            raise ValueError(
                f"Can't go from current state '{self.status}' to '{new_status}'. "
                + f"Allowed transitions from {self.status}: {allowed}."
            )
        self.status = new_status

    def wait(self):
        """Updates the task status to WAITING"""
        self._update_status(Status.WAITING)

    def claim(self):
        """Updates the task status to CLAIMED"""
        self._update_status(Status.CLAIMED)

    def put_in_progress(self):
        """Updates the task status to IN_PROGRESS and sets the time_started field to the
        current time
        """
        self._update_status(Status.IN_PROGRESS)
        self.time_started = datetime.now().isoformat()

    def succeed(self, result: TaskResult):
        """Updates the task status to SUCCESS, sets the time_completed field to the
        current time, and sets the result field with the result from blueapi

        Args:
            result (TaskResult): The result of the task from blueapi
        """
        self._update_status(Status.SUCCESS)
        self.result = result
        self.time_completed = datetime.now().isoformat()

    def fail(self, errors: list[str | TaskError] | None = None):
        """Updates the task status to ERROR, sets the time_completed field to the
        current time, and adds any errors to the errors field.

        Args:
            errors (list[str  |  TaskError] | None, optional): List of errors that
            occurred when trying to run the task. Defaults to None.
        """
        self._update_status(Status.ERROR)
        self.time_completed = datetime.now().isoformat()
        if errors:
            self.errors.extend(errors)
