import pytest

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
