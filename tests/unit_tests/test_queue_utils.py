import pytest
from blueapi.service.model import TaskRequest

from daq_queuing_service.task_queue.queue_utils import (
    TaskPositionError,
    validate_task_position,
)
from daq_queuing_service.task_queue.task import Task, TaskID, TaskRules


@pytest.mark.parametrize(
    "rules, queue",
    [
        (
            TaskRules(run_after={"t0", "t1", "t2"}),
            ["t0", "t1", "task_being_tested", "t2"],
        ),
        (
            TaskRules(run_after={"t0", "t5"}),
            ["task_being_tested", "t0", "t1", "t2"],
        ),
        (
            TaskRules(run_before={"t2"}, run_after={"t3"}, movable=False),
            ["t0", "t1", "task_being_tested", "t2", "t3"],
        ),
    ],
)
def test_validate_task_position_raises_error_if_task_too_early_in_queue(
    rules: TaskRules, queue: list[TaskID]
):
    positions = {task_id: i for i, task_id in enumerate(queue)}
    task = Task(
        id="task_being_tested",
        rules=rules,
        experiment=TaskRequest(name="", params={}, instrument_session=""),
    )

    with pytest.raises(TaskPositionError) as exc:
        validate_task_position(task, positions)

    assert (
        f"Task cannot be in position {positions[task.id]} as it would be before"
        in str(exc.value)
    )


@pytest.mark.parametrize(
    "rules, queue",
    [
        (
            TaskRules(run_before={"t0", "t1", "t2"}),
            ["t0", "t1", "task_being_tested", "t2"],
        ),
        (
            TaskRules(run_before={"t0", "t5"}),
            ["t0", "task_being_tested", "t1", "t2"],
        ),
        (
            TaskRules(run_before={"t2", "t1"}, run_after={"t0"}, movable=False),
            ["t0", "t1", "task_being_tested", "t2", "t3"],
        ),
    ],
)
def test_validate_task_position_raises_error_if_task_too_late_in_queue(
    rules: TaskRules, queue: list[TaskID]
):
    positions = {task_id: i for i, task_id in enumerate(queue)}
    task = Task(
        id="task_being_tested",
        rules=rules,
        experiment=TaskRequest(name="", params={}, instrument_session=""),
    )

    with pytest.raises(TaskPositionError) as exc:
        validate_task_position(task, positions)

    assert (
        f"Task cannot be in position {positions[task.id]} as it would be after"
        in str(exc.value)
    )


@pytest.mark.parametrize(
    "rules, queue",
    [
        (
            TaskRules(run_before={"t2", "t3"}, run_after={"t0", "t1"}, movable=False),
            ["t0", "t1", "task_being_tested", "t2", "t3"],
        ),
        (
            TaskRules(
                run_before={"t2", "t3", "t4"}, run_after={"t0", "t1", "t5", "t6"}
            ),
            ["t0", "t1", "task_being_tested", "t2", "t3"],
        ),
        (
            TaskRules(run_before=set(), run_after=set()),
            ["t0", "t1", "task_being_tested", "t2", "t3"],
        ),
        (
            TaskRules(run_before={"t1", "t2"}),
            ["task_being_tested", "t0", "t1", "t2", "t3"],
        ),
        (
            TaskRules(run_after={"t1", "t2"}),
            ["t0", "t1", "t2", "t3", "task_being_tested"],
        ),
    ],
)
def test_validate_task_position_does_not_raise_if_rules_met(
    rules: TaskRules, queue: list[TaskID]
):
    positions = {task_id: i for i, task_id in enumerate(queue)}
    task = Task(
        id="task_being_tested",
        rules=rules,
        experiment=TaskRequest(name="", params={}, instrument_session=""),
    )
    validate_task_position(task, positions)
