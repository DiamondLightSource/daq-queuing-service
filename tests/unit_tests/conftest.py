import pytest
from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskError, TaskResult

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.broadcaster import Broadcaster
from daq_queuing_service.plugins.construct_task_request import (
    construct_blueapi_call_list,
)
from daq_queuing_service.task import ExperimentDefinition, Task, TaskWithPosition
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
    queue = TaskQueue(convert=construct_blueapi_call_list, broadcaster=Broadcaster())
    await queue.update_state(paused=False)
    await queue.add_tasks(tasks)
    return queue


@pytest.fixture
async def task_queue_one_to_many(tasks: list[Task]):

    def construct_blueapi_call_list(
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:
        call_list: list[BlueapiCall] = []
        for task in queue:
            call_list.append(
                BlueapiCall(
                    parent_task_id=task.id,
                    task_request=TaskRequest(
                        name=task.experiment_definition.plan_name,
                        params=task.experiment_definition.params,
                        instrument_session=task.experiment_definition.instrument_session,
                    ),
                )
            )
            call_list.append(
                BlueapiCall(
                    parent_task_id=task.id,
                    task_request=TaskRequest(
                        name=task.experiment_definition.plan_name,
                        params=task.experiment_definition.params,
                        instrument_session=task.experiment_definition.instrument_session,
                    ),
                )
            )
        return call_list

    queue = TaskQueue(convert=construct_blueapi_call_list, broadcaster=Broadcaster())
    await queue.update_state(paused=False)
    await queue.add_tasks(tasks)
    return queue


@pytest.fixture
async def task_queue_claimed(task_queue: TaskQueue):
    _ = task_queue.get_next_call_once_available()
    return task_queue


@pytest.fixture
async def task_queue_in_progress(task_queue: TaskQueue):
    first_call = await task_queue.get_next_call_once_available()
    first_call.put_in_progress()
    return task_queue


@pytest.fixture
async def task_queue_with_history(task_queue: TaskQueue):
    for i in range(2):
        call = await task_queue.get_next_call_once_available()
        call.put_in_progress()
        if i % 2:
            await task_queue.complete_call(
                call, TaskResult(result=None, type="NoneType")
            )
        else:
            await task_queue.fail_call(
                call, [TaskError(type="ValueError", message="Error during plan")]
            )
    # By this point should have 3 tasks in queue and 2 in history
    for i, call in enumerate(task_queue._call_history):
        # Real timestamps will break tests
        call.time_started = f"2026-04-17T15:0{i}:00.000000"
        call.time_completed = f"2026-04-17T15:0{i}:59.000000"

    call = await task_queue.get_next_call_once_available()
    call.put_in_progress()
    call.time_started = "2026-04-17T15:02:00.000000"
    return task_queue
