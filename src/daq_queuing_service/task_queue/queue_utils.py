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


class TaskIsCompleteError(QueueError):
    pass
