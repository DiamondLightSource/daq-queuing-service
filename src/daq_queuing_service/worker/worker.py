import asyncio
import logging
from collections.abc import Callable
from functools import partial

from blueapi.client.event_bus import AnyEvent
from blueapi.client.rest import (
    BlueskyRemoteControlError,
    InvalidParametersError,
    ServiceUnavailableError,
    UnknownPlanError,
)
from blueapi.core import DataEvent
from blueapi.service.model import TaskRequest
from blueapi.worker import ProgressEvent, TaskStatus, WorkerEvent, WorkerState
from blueapi.worker.event import TaskError, TaskResult

from daq_queuing_service.blueapi_adapter import BlueapiClientAdapter
from daq_queuing_service.task import ExperimentDefinition, Status, Task
from daq_queuing_service.task_queue.queue import TaskQueue

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class QueueWorker:
    def __init__(
        self,
        queue: TaskQueue,
        blueapi_client: BlueapiClientAdapter,
        task_request_constructor: Callable[[ExperimentDefinition], TaskRequest],
        poll_time_s: float = 1.0,
    ):
        self.poll_time_s = poll_time_s
        self._queue = queue
        self._client = blueapi_client
        self._task_request_constructor = task_request_constructor

    async def run_loop(self):
        while True:
            next_task = await self._wait_for_next_task()
            await self._process_task(next_task)

    async def _wait_for_next_task(self):
        while True:
            await self._queue.wait_until_task_available()
            result = self._client.get_state()
            if result.value == WorkerState.IDLE:
                break
            LOGGER.info(
                f"Waiting for BlueAPI worker to be IDLE, currently {result.value}"
            )
            await asyncio.sleep(self.poll_time_s)
        return await self._queue.claim_next_task_once_available()

    async def _process_task(self, task: Task):
        task_request = self._task_request_constructor(task.experiment_definition)
        LOGGER.info(f"Sending task {task.id} to BlueAPI")

        result = self._client.run_task(
            task_request, on_event=partial(self._on_blueapi_event, task=task)
        )

        if result.error:
            await self._handle_run_task_error(task, result.error)
            return

        assert result.value
        task_status: TaskStatus = result.value
        assert task_status.result
        match task_status.result:
            case TaskResult():
                LOGGER.debug(
                    f"Task {task.id} completed succesfully:  {task_status.result}"
                )
                await self._queue.complete_task(task, task_status.result)
            case TaskError():
                LOGGER.debug(f"Task {task.id} failed: {task_status.result}")
                await self._queue.fail_task(task, [task_status.result])

    @staticmethod
    def _on_blueapi_event(event: AnyEvent, task: Task):
        match event:
            case WorkerEvent() as worker_event:
                if task.status != Status.IN_PROGRESS:
                    assert worker_event.task_status
                    task.blueapi_id = worker_event.task_status.task_id
                    LOGGER.info(
                        f"Task {task.id} is in progress, blueapi ID: " + task.blueapi_id
                    )
                    task.put_in_progress()
            case ProgressEvent():
                pass

            case DataEvent():
                pass

    async def _handle_run_task_error(
        self,
        task: Task,
        error: InvalidParametersError
        | UnknownPlanError
        | ServiceUnavailableError
        | BlueskyRemoteControlError
        | ServiceUnavailableError,
    ):
        match error:
            case InvalidParametersError():
                await self._queue.fail_task(
                    task,
                    errors=[str(error) for error in error.errors],
                )
            case UnknownPlanError():
                await self._queue.fail_task(task, ["Unknown plan", str(error)])
            case BlueskyRemoteControlError() | ServiceUnavailableError():
                # We get this error if the blueapi worker is busy or unavailable
                await self._queue.return_task_to_queue(task)
