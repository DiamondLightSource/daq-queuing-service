import pytest
from blueapi.worker.event import TaskError, TaskResult

from daq_queuing_service.task import ExperimentDefinition, Task
from daq_queuing_service.task_queue.queue import TaskQueue


@pytest.fixture
def tasks() -> list[Task]:
    return [
        Task(
            experiment_definition=ExperimentDefinition(
                plan_name="test", sample_id=str(i), params={}, instrument_session=""
            ),
            id=str(i),
        )
        for i in range(5)
    ]


@pytest.fixture
async def task_queue(tasks: list[Task]):
    queue = TaskQueue()
    await queue.update_state(paused=False)
    await queue.add_tasks(tasks)
    return queue


@pytest.fixture
async def task_queue_claimed(task_queue: TaskQueue):
    first_task_id = task_queue._queue[0]
    first_task = task_queue._tasks[first_task_id]
    first_task.claim()
    return task_queue


@pytest.fixture
async def task_queue_in_progress(task_queue_claimed: TaskQueue):
    first_task_id = task_queue_claimed._queue[0]
    first_task = task_queue_claimed._tasks[first_task_id]
    first_task.blueapi_id = "blueapi_id_0"
    first_task.put_in_progress()
    return task_queue_claimed


@pytest.fixture
async def task_queue_with_history(task_queue: TaskQueue):
    for i in range(2):
        task = await task_queue.claim_next_task_once_available()
        task.blueapi_id = f"blueapi_id_{i}"
        task.put_in_progress()
        if i % 2:
            await task_queue.complete_task(
                task, TaskResult(result=None, type="NoneType")
            )
        else:
            await task_queue.fail_task(
                task, [TaskError(type="ValueError", message="Error during plan")]
            )
    # By this point should have 3 tasks in queue and 2 in history
    for i, task_id in enumerate(task_queue._history):
        # Real timestamps will break tests
        task_queue._tasks[task_id].time_started = f"2026-04-17T15:0{i}:00.000000"
        task_queue._tasks[task_id].time_completed = f"2026-04-17T15:0{i}:59.000000"

    task = await task_queue.claim_next_task_once_available()
    task.blueapi_id = f"blueapi_id_{2}"
    task.put_in_progress()
    task.time_started = "2026-04-17T15:02:00.000000"
    return task_queue
