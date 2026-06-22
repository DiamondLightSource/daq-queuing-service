import asyncio
import logging
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import Any, Literal

from blueapi.worker.event import TaskError, TaskResult
from pydantic import BaseModel

from daq_queuing_service.app._load_converter import Converter
from daq_queuing_service.blueapi_interaction.blueapi_call import (
    BlueapiCall,
    BlueapiCallResponse,
    CallStatus,
)
from daq_queuing_service.broadcaster import Broadcaster, Event
from daq_queuing_service.task import Status, Task, TaskWithPosition
from daq_queuing_service.task_queue.queue_utils import (
    NegativePositionError,
    TaskIdInUseError,
    TaskInProgressError,
    TaskNotClaimedError,
    TaskNotFoundError,
    TaskNotInQueueError,
)

LOGGER = logging.getLogger(__name__)


class TaskRegistry(dict[str, Task]):
    def __missing__(self, task_id: str) -> Task:
        raise TaskNotFoundError(f"No task found matching id: {task_id}")


class QueueState(BaseModel):
    paused: bool


class Modifying(asyncio.Condition):
    def __init__(self, on_exit: Callable[[], Any]):
        super().__init__()
        self._on_exit = on_exit

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ):
        self._on_exit()
        return await super().__aexit__(exc_type, exc, tb)


QUEUE_EVENTS = Literal[
    "state_update",
    "queue_update",
    "history_update",
    "tasks_update",
    "call_queue_update",
    "call_history_update",
]


class TaskQueue:
    def __init__(self, convert: Converter, broadcaster: Broadcaster[QUEUE_EVENTS]):
        self._tasks: TaskRegistry = TaskRegistry()
        self._queue: list[str] = []
        self._history: list[str] = []
        self._call_queue: list[BlueapiCall] = []
        self._call_history: list[BlueapiCall] = []
        self._queue_history: list[BlueapiCall] = []
        self._state: QueueState = QueueState(paused=True)
        self._convert = convert
        self._modifying = Modifying(on_exit=self._sync)
        self._broadcaster = broadcaster

    def _sync(self):
        """Syncs the task queue with the call queue, applying a conversion from queue of
        tasks to a queue of blueapi calls. This is called every time the queue is
        modified, and also right before a call is popped off the front of the queue.
        """
        LOGGER.debug("Syncing")
        for task_id in list(self._queue):
            task = self._tasks[task_id]
            if task.status == Status.COMPLETE:
                # Move task from queue to history if all blueapi calls complete
                self._queue.remove(task_id)
                self._history.append(task_id)
            elif task.status != Status.IN_PROGRESS:
                # If task is not in progress calls will be re-calculated
                task.blueapi_calls = []

        self._call_queue = [
            # Persist calls which aren't complete but who's parent task is in progress
            # Once a task is in progress it is not provided to the converter
            # More work needed to allow for interleaved calls from different tasks,
            # and for in progress tasks to inform conversion
            call
            for call in self._call_queue
            if call.parent_task_id
            and call.parent_task_id in self._queue
            and self._tasks[call.parent_task_id].status == Status.IN_PROGRESS
            and call.status not in (CallStatus.SUCCESS, CallStatus.ERROR)
        ]

        new_calls = self._convert(
            [task for task in self._get_queue() if task.status == Status.QUEUED],
            self._get_history(),
            self._queue_history,
        )
        self._call_queue.extend(new_calls)

        for call in new_calls:
            # Add children to parent tasks
            if call.parent_task_id:
                self._tasks[call.parent_task_id].blueapi_calls.append(call)

        self._broadcast_changes()
        self._modifying.notify_all()

    def _broadcast_changes(self):
        queue = self._get_queue()
        history = self._get_history()

        # broadcast only does anything if there have been changes
        self._broadcaster.broadcast(Event(type="queue_update", data=queue))
        self._broadcaster.broadcast(Event(type="history_update", data=history))
        self._broadcaster.broadcast(Event(type="tasks_update", data=history + queue))
        self._broadcaster.broadcast(
            Event(type="call_queue_update", data=self._get_call_queue())
        )
        self._broadcaster.broadcast(
            Event(type="call_history_update", data=self._get_call_history())
        )

    async def get_next_call_once_available(self) -> BlueapiCall:
        """Waits until a call is available before returning the call. A call is
        available if it's at the top of the call queue, is not already in progress or
        claimed, and the queue is not paused.

        Returns:
            BlueapiCall: The call at the top of the queue
        """
        async with self._modifying:
            while not self._task_available():
                await self._modifying.wait()
            self._sync()  # Do conversion here to ensure conditions are up to date
            call = self._call_queue[0]
            call.claim()
        LOGGER.info(f"Plan {call} has been claimed")
        return call

    async def wait_until_call_available(self):
        """Waits until a task is available before returning. A task is
        available if it's at the top of the queue, is not already in progress or
        claimed, and the queue is not paused.
        """
        async with self._modifying:
            while not self._task_available():
                await self._modifying.wait()

    async def return_call_to_queue(self, call: BlueapiCall):
        """Returns a task to the queue that had previously been claimed

        Args:
            task (Task): The task to return

        Raises:
            TaskNotClaimedError: Raised if the task's status is not 'Claimed'
        """
        self._check_call_valid_to_be_returned(call)
        async with self._modifying:
            match call.status:
                case CallStatus.CLAIMED:
                    assert call == self._call_queue[0]
                    call.wait()
                case _:
                    raise TaskNotClaimedError(
                        f"Cannot return call {call}, "
                        + f"it's status is {call.status}."
                    )
        LOGGER.info(f"Call {call} has been returned to the queue")

    async def complete_call(self, call: BlueapiCall, result: TaskResult):
        """Sets a task to complete, removes it from the queue and adds it to history

        Args:
            task (Task): Task to be completed
            result (TaskResult): The result of the task from blueapi
        """
        async with self._modifying:
            self._check_call_valid_to_be_returned(call)
            call.succeed(result)
            self._call_history.append(call)
        LOGGER.info(f"Plan {call} has been completed successfully: {result}")

    async def fail_call(
        self, call: BlueapiCall, errors: list[str | TaskError] | None = None
    ):
        """Sets a task to failed, removes it from the call queue and adds it to history.

        Args:
            task (Task): The task to fail
            errors (list[str  |  TaskError] | None, optional): A list of errors that
            occurred when trying to run the task. Defaults to None.
        """
        async with self._modifying:
            self._check_call_valid_to_be_returned(call)
            call.fail(errors)
            self._call_history.append(call)
        LOGGER.error(f"Call {call} has failed with the following errors: {errors}")

    async def get_task_by_id(self, task_id: str) -> TaskWithPosition:
        """Returns a task based on it's task ID

        Args:
            task_id (str): Task ID of the task

        Returns:
            TaskWithPosition: A copy of the task with a position field.

        Raises:
            TaskNotFoundError: Raised if the no task exists with the requested task ID.
        """
        # Returns copy so don't have to be worried about caller modifying task.
        async with self._modifying:
            return self._get_task_by_id(task_id)

    def _get_task_by_id(self, task_id: str) -> TaskWithPosition:
        task = self._tasks[task_id]
        position = self._queue.index(task.id) if task.id in self._queue else None
        return TaskWithPosition.from_task(task, position)

    async def get_task_by_position(self, position: int) -> TaskWithPosition | None:
        """Return a task based on it's position in the queue

        Args:
            position (int): The position of the task to be returned.

        Returns:
            TaskWithPosition | None: A copy of the task with a position field, or None
            if no task exists at the requested position.
        """
        # Returns copy so don't have to be worried about caller modifying task.
        async with self._modifying:
            if position < -self.length or position >= self.length:
                return None
            return self._get_task_by_id(self._queue[position])

    async def get_queue(self) -> list[TaskWithPosition]:
        """Get the entire queue (not including history)

        Returns:
            list[TaskWithPosition]: A list of the tasks in the queue, in the order they
            will be run in.
        """
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._modifying:
            return self._get_queue()

    async def get_history(self) -> list[TaskWithPosition]:
        """Get the history list.

        Returns:
            list[TaskWithPosition]: A list of the tasks in the history list, in
            chronological order.
        """
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._modifying:
            return self._get_history()

    async def get_tasks(self) -> list[TaskWithPosition]:
        """Get all the tasks in the queue and history list.

        Returns:
            list[TaskWithPosition]: A list of tasks in chronological order, starting
            with the history.
        """
        # Returns copies so don't have to be worried about caller modifying tasks.
        async with self._modifying:
            return self._get_history() + self._get_queue()

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        """Add tasks to the queue. Can specify a position to add the tasks in at.
        This position will apply to the first task in the list, with each subsequent
        task having a position of 1 more than the previous. By default adds tasks to the
        end of the queue.


        Args:
            tasks (list[Task]): List of tasks to add
            position (int | None, optional): Position of the tasks. Defaults to None.
        """
        async with self._modifying:
            self._validate_new_tasks(tasks)
            if position is not None:
                position = self._get_valid_position(position)
            self._add_tasks(tasks, position)
        LOGGER.info(f"Successfully added tasks to queue: {[task.id for task in tasks]}")

    async def move_task(self, task_id: str, position: int) -> int:
        """Move a task into a different position. If the requested position is
        unavailable, will move to the closest available position. If the requested task
        cannot be moved because it is complete or in progress, an error will be raised.

        Args:
            task_id (str): ID of the task to be moved
            new_position (int): New position of the task

        Returns:
            int: The new position of the task (may be different to what was requested)
        """
        async with self._modifying:
            self._validate_tasks_for_move_or_deletion([task_id])
            position = self._get_valid_position(position)
            self._remove_tasks_from_queue([task_id])
            self._queue[position:position] = [task_id]
            new_position = self._queue.index(task_id)
        LOGGER.info(f"Succesfully moved task {task_id} to position {new_position}")
        return new_position

    async def cancel_tasks(self, task_ids: Sequence[str]) -> list[TaskWithPosition]:
        """Remove tasks from the queue. If one or more of the requested tasks cannot be
        cancelled as they are complete or in progress, an error will be raised and none
        of the requested tasks will be cancelled.

        Args:
            task_ids (Sequence[str]): List of task IDs to cancel

        Returns:
            list[TaskWithPosition]: List of the task objects that were removed from the
            queue.
        """
        async with self._modifying:
            task_ids = list(task_ids)
            self._validate_tasks_for_move_or_deletion(task_ids)
            self._remove_tasks_from_queue(task_ids)
            tasks = self._remove_tasks_from_registry(task_ids)
            for task in tasks:
                task.cancel()
        LOGGER.info(f"Succesfully cancelled tasks: {task_ids}")
        return [TaskWithPosition.from_task(task) for task in tasks]

    async def clear_history(self):
        """Clears the history list. Any task in the history list at the time will be
        deleted permanently and inaccessible.
        """
        async with self._modifying:
            for task_id in self._history:
                self._tasks.pop(task_id)
            self._history.clear()
        LOGGER.info("Succesfully cleared history")

    async def update_state(self, paused: bool | None = None) -> QueueState:
        """Update the state of the queue.

        Args:
            paused (bool | None, optional): Whether or not the queue should be paused.

        Returns:
            QueueState: The new state of the queue.
        """
        async with self._modifying:
            self._state = QueueState(
                paused=self._state.paused if paused is None else paused
            )
            self._broadcaster.broadcast(Event(type="state_update", data=self._state))
        LOGGER.info(f"Succesfully updated queue state to {self._state}")
        return self._state

    @property
    def state(self):
        return self._state

    @property
    def length(self):
        return len(self._queue)

    def _task_available(self) -> bool:
        """Predicate that determines whether the queue has a task available. This
        returns True if the first task in the queue has a status of WAITING, and the
        queue is not paused.

        Returns:
            bool: Whether or not the queue has a task available.
        """
        if self._state.paused or not self._call_queue:
            return False
        return self._call_queue[0].status == CallStatus.WAITING

    def _check_call_valid_to_be_returned(self, call: BlueapiCall):
        # Check caller has actual task object not copy
        # This ensures the caller has claimed the task, reducing the chance a task is
        # returned that is actually still being run/modified by a different process.
        # However if the worker crashes we then lose the Task object and can't return
        # the task? Needs discussion with others.
        assert call is self._call_queue[0]
        assert call.parent_task_id in self._queue, (
            f"This call has no parent task: {call}"
        )

    def _get_valid_position(self, position: int) -> int:
        if position < 0:
            raise NegativePositionError(f"Position must be >= 0, got {position}")
        if (  # if position 0 requested but a task is in progress, return position 1
            position == 0
            and self.length
            and self._tasks[self._queue[0]].status != Status.QUEUED
        ):
            return 1
        return position

    def _validate_new_tasks(self, tasks: list[Task]):
        for task in tasks:
            if task.id in self._tasks:
                raise TaskIdInUseError(f"Task ID '{task.id}' already in use!")

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self._queue.extend(task_ids)
        else:
            self._queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks_from_queue(self, task_ids: list[str]) -> list[str]:
        #  Only removes tasks in the queue (not history or registry)
        def should_be_removed(task_id: str):
            return (
                task_id in self._queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        self._queue = [task_id for task_id in self._queue if task_id not in removed_ids]

        return removed_ids

    def _remove_tasks_from_registry(self, task_ids: list[str]) -> list[Task]:
        # Should remove tasks from queue/history before removing from registry
        def should_be_removed(task_id: str) -> bool:
            return (
                task_id in self._tasks
                and self._tasks[task_id].status != Status.IN_PROGRESS
                and task_id not in self._queue
                and task_id not in self._history
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        removed = [self._tasks[task_id] for task_id in removed_ids]
        self._tasks = TaskRegistry(
            {
                task_id: task
                for task_id, task in self._tasks.items()
                if task.id not in removed_ids
            }
        )
        return removed

    def _validate_tasks_for_move_or_deletion(self, task_ids: list[str]):
        for task_id in task_ids:
            task = self._tasks[task_id]
            if task_id not in self._queue:
                raise TaskNotInQueueError(f"Task {task_id} isn't present in queue")
            if task.status != Status.QUEUED:
                raise TaskInProgressError(
                    f"Cannot move task '{task_id}', it is currently in progress!"
                )

    def _get_queue(self) -> list[TaskWithPosition]:
        return [
            TaskWithPosition.from_task(self._tasks[task_id], i)
            for i, task_id in enumerate(self._queue)
        ]

    def _get_history(self) -> list[TaskWithPosition]:
        return [
            TaskWithPosition.from_task(self._tasks[task_id])
            for task_id in self._history
        ]

    async def get_call_queue(self) -> list[BlueapiCallResponse]:
        async with self._modifying:
            return self._get_call_queue()

    def _get_call_queue(self) -> list[BlueapiCallResponse]:
        return [call.to_response() for call in self._call_queue]

    async def get_call_history(self) -> list[BlueapiCallResponse]:
        async with self._modifying:
            return self._get_call_history()

    def _get_call_history(self) -> list[BlueapiCallResponse]:
        return [call.to_response() for call in self._call_history]
