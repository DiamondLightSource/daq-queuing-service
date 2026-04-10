import time

from blueapi.client import BlueapiRestClient
from blueapi.client.rest import ServiceUnavailableError
from blueapi.config import RestConfig
from blueapi.service.model import TaskRequest, WorkerTask
from blueapi.worker import WorkerState
from pydantic import HttpUrl

from daq_queuing_service.queue import TaskQueue
from daq_queuing_service.task import Task

I15_1_BLUEAPI_URL = "https://i15-1-blueapi.diamond.ac.uk/"


def construct_blueapi_task_request(task: Task) -> TaskRequest: ...


class QueueWorker:
    def __init__(
        self, queue: TaskQueue, blueapi_url: HttpUrl, poll_time_s: float = 1.0
    ):
        self.queue = queue
        self.poll_time_s = poll_time_s
        self._url = blueapi_url
        self._client = BlueapiRestClient(config=RestConfig(url=blueapi_url))

    async def run_loop(self):
        while True:
            await self.queue.wait_until_task_available()
            self._wait_until_blueapi_is_idle()
            next_task = await self.queue.claim_next_task_once_available()
            await self.send_task_to_blue_api(next_task)

    def _wait_until_blueapi_is_idle(self):
        while self._client.get_state() != WorkerState.IDLE:
            time.sleep(self.poll_time_s)

    async def send_task_to_blue_api(self, task: Task, timeout_s: int = 600):
        task_request = construct_blueapi_task_request(task)
        response = self._client.create_task(task_request)
        blueapi_task_id = response.task_id
        try:
            self._client.update_worker_task(WorkerTask(task_id=blueapi_task_id))
        except Exception as e:
            # logger.info("")
            if isinstance(e, ServiceUnavailableError):
                await self.queue.return_task_to_queue(task)
                raise e
            else:
                await self.queue.complete_task(task, error=str(e))
                raise e
