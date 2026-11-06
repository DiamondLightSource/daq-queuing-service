from daq_queuing_service.task_queue.task import Task, TaskID


class QueueError(Exception):
    pass


class TaskInProgressError(QueueError):
    pass


class TaskNotFoundError(QueueError, KeyError):
    pass


class TaskNotInQueueError(QueueError):
    pass


class TaskIdInUseError(QueueError):
    pass


class NegativePositionError(QueueError):
    pass


class TaskNotClaimedError(QueueError):
    pass


class TaskNotMovableError(QueueError):
    pass


class TaskPositionError(QueueError):
    pass


def validate_task_position(task: Task, positions: dict[TaskID, int]):
    task_position = positions[task.id]

    for must_be_before_id in task.rules.run_before:
        must_be_before = positions.get(must_be_before_id)
        if must_be_before is not None and task_position > must_be_before:
            raise TaskPositionError(
                f"Task cannot be in position {task_position} as it would be after"
                + f" {must_be_before_id} in position {must_be_before}. See the "
                + f"following rules for task {task.id}: {task.rules}"
            )

    for must_be_after_id in task.rules.run_after:
        must_be_after = positions.get(must_be_after_id)
        if must_be_after is not None and task_position < must_be_after:
            raise TaskPositionError(
                f"Task cannot be in position {task_position} as it would be before"
                + f" {must_be_after_id} in position {must_be_after}. See the following "
                + f"rules for task {task.id}: {task.rules}"
            )
