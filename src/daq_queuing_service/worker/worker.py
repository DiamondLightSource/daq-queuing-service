import asyncio
import logging

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import InvalidParametersError, ServiceUnavailableError
from blueapi.config import RestConfig
from blueapi.service.model import TaskRequest, TrackableTask, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.queue.queue import TaskQueue
from daq_queuing_service.task import Task

LOGGER = logging.getLogger(__name__)


def construct_blueapi_task_request(task: Task) -> TaskRequest:
    return TaskRequest(
        name=task.experiment_definition.plan_name,
        params=task.experiment_definition.params,
        instrument_session=task.experiment_definition.instrument_session,
    )


class QueueWorker:
    def __init__(
        self, queue: TaskQueue, blueapi_url: HttpUrl, poll_time_s: float = 1.0
    ):
        self.poll_time_s = poll_time_s
        self._queue = queue
        self._url = blueapi_url
        self._client = BlueapiRestClient(config=RestConfig(url=blueapi_url))

    async def run_loop(self):
        while True:
            await self._queue.wait_until_task_available()
            if not await self._is_blue_api_idle():
                await asyncio.sleep(self.poll_time_s)
                continue
            next_task = await self._queue.claim_next_task_once_available()
            await self._send_task_to_blue_api_and_wait_for_completion(next_task)

    async def _is_blue_api_idle(self) -> bool:
        return self._client.get_state() == WorkerState.IDLE

    async def _send_task_to_blue_api_and_wait_for_completion(
        self, task: Task, timeout_s: int = 600
    ):
        task_request = construct_blueapi_task_request(task)

        if not task.blueapi_id:
            try:
                response = self._client.create_task(task_request)
            except InvalidParametersError as e:
                # Validation issue - task will not work if retried so should cancel task
                await self._queue.fail_task(
                    task, errors=[str(error) for error in e.errors]
                )
                LOGGER.error(e)
                return
            task.blueapi_id = response.task_id

        try:
            self._client.update_worker_task(WorkerTask(task_id=task.blueapi_id))
        except ServiceUnavailableError as e:
            # Issue with blueapi worker state or connection - should retry task later
            await self._queue.return_task_to_queue(task)
            LOGGER.error(e)
            return
        LOGGER.info(f"Task {task.id} is in progress, blueapi ID: {task.blueapi_id}")
        task.put_in_progress()

        blueapi_task = await self._get_blueapi_task_once_complete(
            task.blueapi_id, timeout_s
        )

        if blueapi_task.errors:
            await self._queue.fail_task(task, blueapi_task.errors)
        else:
            await self._queue.complete_task(task)

    async def _get_blueapi_task_once_complete(
        self, blueapi_task_id: str, timeout_s: int
    ) -> TrackableTask:
        blueapi_task: TrackableTask = self._client.get_task(blueapi_task_id)
        while not blueapi_task.is_complete:
            await asyncio.sleep(self.poll_time_s)
            blueapi_task = self._client.get_task(blueapi_task_id)
        return blueapi_task
