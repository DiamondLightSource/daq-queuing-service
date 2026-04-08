import pytest

from daq_queuing_service.queue import TaskQueue
from daq_queuing_service.task import ExperimentDefinition, Status, Task

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def tasks() -> list[Task]:
    return [
        Task(experiment_definition=ExperimentDefinition(sample_id=str(i)), id=str(i))
        for i in range(5)
    ]


@pytest.fixture
async def task_queue_in_progress(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    first_task_id = queue.queue[0]
    first_task = queue._tasks[first_task_id]  # type: ignore  # noqa
    first_task.update_status(Status.IN_PROGRESS)
    return queue


async def test_add_tasks_works_as_expected(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    assert queue.queue == ["0", "1", "2", "3", "4"]
    assert set(queue._tasks.keys()) == {"0", "1", "2", "3", "4"}  # type: ignore  # noqa


@pytest.mark.parametrize(
    "task_to_move, new_position, expected_order",
    [
        [2, 2, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        [5, 2, [0, 1, 5, 2, 3, 4, 6, 7, 8, 9]],
        [2, 3, [0, 1, 3, 2, 4, 5, 6, 7, 8, 9]],
        [9, 0, [9, 0, 1, 2, 3, 4, 5, 6, 7, 8]],
        [0, 9, [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]],
    ],
)
async def test_move_task_works_as_expected(
    task_to_move: int, new_position: int, expected_order: list[int]
):
    queue = TaskQueue()
    tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id=str(i)), id=str(i))
        for i in range(10)
    ]
    await queue.add_tasks(tasks)
    task = str(task_to_move)
    await queue.move_task(task, new_position)
    result_order = [int(task_id) for task_id in queue.queue]
    assert result_order == expected_order


async def test_move_task_to_position_0_moves_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("4", 0)
    assert task_queue_in_progress.queue == ["0", "4", "1", "2", "3"]


async def test_move_task_does_not_move_task_that_is_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("0", 3)
    assert task_queue_in_progress.queue == ["0", "1", "2", "3", "4"]


async def test_remove_tasks_works_as_expected(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    await queue.remove_tasks(["4", "2"])
    assert queue.queue == ["0", "1", "3"]


async def test_remove_tasks_does_not_remove_task_that_is_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.remove_tasks(["0", "2"])
    assert task_queue_in_progress.queue == ["0", "1", "3", "4"]


async def test_remove_tasks_does_not_error_if_wrong_task_id_used(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    await queue.remove_tasks(["4", "2", "10"])
    assert queue.queue == ["0", "1", "3"]
