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
async def task_queue(tasks: list[Task]):
    queue = TaskQueue()
    await queue.add_tasks(tasks)
    return queue


@pytest.fixture
async def task_queue_in_progress(task_queue: TaskQueue):
    first_task_id = task_queue.queue[0]
    first_task = task_queue._tasks[first_task_id]  # type: ignore  # noqa
    first_task.update_status(Status.IN_PROGRESS)
    return task_queue


@pytest.fixture
async def task_queue_with_history(task_queue: TaskQueue):
    for _ in range(2):
        task = await task_queue.claim_next_task_once_available()
        await task_queue.complete_task(task)
    # By this point should have 3 tasks in queue and 2 in history
    for i, task_id in enumerate(task_queue.history):
        # Real timestamps will break tests
        task_queue._tasks[task_id].time_started = 1 + i
        task_queue._tasks[task_id].time_completed = 1 + i + 0.9
    return task_queue


async def test_add_tasks_adds_to_end_when_no_position_given(task_queue: TaskQueue):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task])
    assert task_queue.queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_tasks_with_position_works_as_expected(task_queue: TaskQueue):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task], 2)
    assert task_queue.queue == ["0", "1", "new", "2", "3", "4"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_tasks_adds_to_the_end_if_position_bigger_than_queue_length(
    task_queue: TaskQueue,
):
    new_task = Task(
        experiment_definition=ExperimentDefinition(sample_id="new"), id="new"
    )
    await task_queue.add_tasks([new_task], 20)
    assert task_queue.queue == ["0", "1", "2", "3", "4", "new"]
    assert set(task_queue._tasks.keys()) == {"0", "1", "2", "3", "4", "new"}  # type: ignore  # noqa


async def test_add_task_to_position_0_add_to_position_1_if_first_task_in_progress(
    task_queue_in_progress: TaskQueue,
):
    new_tasks = [
        Task(experiment_definition=ExperimentDefinition(sample_id="new"), id="new"),
        Task(experiment_definition=ExperimentDefinition(sample_id="new_2"), id="new_2"),
    ]
    await task_queue_in_progress.add_tasks(new_tasks, 0)
    assert task_queue_in_progress.queue == ["0", "new", "new_2", "1", "2", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {  # pyright: ignore[reportPrivateUsage]
        "0",
        "1",
        "2",
        "3",
        "4",
        "new",
        "new_2",
    }


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
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "2", "3", "4"}


async def test_move_task_does_not_error_if_wrong_task_id_given(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.move_task("10", 3)
    assert task_queue_in_progress.queue == ["0", "1", "2", "3", "4"]


async def test_remove_tasks_works_as_expected(task_queue: TaskQueue):
    await task_queue.remove_tasks(["4", "2"])
    assert task_queue.queue == ["0", "1", "3"]


async def test_remove_tasks_does_not_remove_task_that_is_in_progress(
    task_queue_in_progress: TaskQueue,
):
    task = await task_queue_in_progress.get_task_by_position(0)
    assert task and task.status == Status.IN_PROGRESS

    await task_queue_in_progress.remove_tasks(["0", "2"])
    assert task_queue_in_progress.queue == ["0", "1", "3", "4"]
    assert set(task_queue_in_progress._tasks.keys()) == {"0", "1", "3", "4"}


async def test_remove_tasks_does_not_error_if_wrong_task_id_used(task_queue: TaskQueue):
    await task_queue.remove_tasks(["4", "2", "10"])
    assert task_queue.queue == ["0", "1", "3"]


async def test_get_queue_only_returns_tasks_in_queue(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.history == ["0", "1"]
    result = await task_queue_with_history.get_queue()
    assert result == [
        '{"experiment_definition":{"sample_id":"2"},"id":"2","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"3"},"id":"3","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"4"},"id":"4","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
    ]


async def test_get_history_only_returns_tasks_in_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.queue == ["2", "3", "4"]
    result = await task_queue_with_history.get_history()
    assert result == [
        '{"experiment_definition":{"sample_id":"0"},"id":"0","status":"Completed","time_started":1.0,"time_completed":1.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"1"},"id":"1","status":"Completed","time_started":2.0,"time_completed":2.9,"errors":[]}',
    ]


async def test_get_tasks_returns_tasks_in_queue_and_history(
    task_queue_with_history: TaskQueue,
):
    assert task_queue_with_history.queue == ["2", "3", "4"]
    assert task_queue_with_history.history == ["0", "1"]
    result = await task_queue_with_history.get_tasks()
    assert result == [
        '{"experiment_definition":{"sample_id":"0"},"id":"0","status":"Completed","time_started":1.0,"time_completed":1.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"1"},"id":"1","status":"Completed","time_started":2.0,"time_completed":2.9,"errors":[]}',
        '{"experiment_definition":{"sample_id":"2"},"id":"2","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"3"},"id":"3","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
        '{"experiment_definition":{"sample_id":"4"},"id":"4","status":"Waiting","time_started":null,"time_completed":null,"errors":[]}',
    ]
