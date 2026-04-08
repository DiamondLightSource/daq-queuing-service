import asyncio

from daq_queuing_service.task import Status, Task, TaskID


class TaskQueue:
    def __init__(self):
        self._tasks: dict[TaskID, Task] = {}
        self.queue: list[TaskID] = []
        self.history: list[TaskID] = []
        self.lock = asyncio.Lock()
        self.condition = asyncio.Condition(self.lock)
        self.paused: bool = False

    def _task_available(self) -> bool:
        if self.paused or not self.queue:
            return False
        return self._tasks[self.queue[0]].status == Status.WAITING

    async def claim_next_task_once_available(self) -> Task:
        async with self.condition:
            while not self._task_available():
                await self.condition.wait()
            task = self._tasks[self.queue[0]]
            task.update_status(Status.IN_PROGRESS)
            self.condition.notify_all()
            return task

    async def complete_task(self, task_id: TaskID):
        async with self.condition:
            task = self._tasks.get(task_id)
            assert task is not None, f"Cannot find task with ID: {task_id}"
            assert task_id in self.queue, f"This task is not in the queue: {task}"
            assert self.queue[0] == task_id, (
                f"This task is not at the front of the queue: {task}"
            )
            assert task.status == Status.IN_PROGRESS, (
                f"This task is not currently in progress: {task}"
            )
            self.queue.pop(0)
            task.update_status(Status.COMPLETED)
            self.history.insert(0, task_id)
            self.condition.notify_all()

    async def get_task_by_id(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    async def get_task_by_position(self, position: int) -> Task | None:
        return self._tasks[self.queue[position]] if position < self.length else None

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self.condition:
            self._verify_new_tasks(tasks, position)
            self._add_tasks(tasks, position)
            self.condition.notify_all()

    async def move_task(self, task_id: str, position: int):
        async with self.condition:
            task = self._remove_tasks([task_id])
            self._add_tasks(task, position)
            self.condition.notify_all()

    async def remove_tasks(self, task_ids: list[str]) -> list[Task]:
        async with self.condition:
            tasks = self._remove_tasks(task_ids)
            self.condition.notify_all()
            return tasks

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self.queue.extend(task_ids)
        else:
            self.queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks(self, task_ids: list[str]) -> list[Task]:
        #  Only removes tasks in the queue (not history)
        def should_be_removed(task_id: TaskID):
            return (
                task_id in self.queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed = [
            self._tasks[task_id] for task_id in task_ids if should_be_removed(task_id)
        ]
        removed_ids = [task.id for task in removed]
        self.queue = [task_id for task_id in self.queue if task_id not in removed_ids]
        self._tasks = {
            task_id: task
            for task_id, task in self._tasks.items()
            if task_id not in removed_ids
        }
        return removed

    async def pause(self):
        async with self.condition:
            self.paused = True

    async def unpause(self):
        async with self.condition:
            self.paused = False
            self.condition.notify_all()

    @property
    def length(self):
        return len(self.queue)

    def _verify_new_tasks(self, tasks: list[Task], position: int | None):
        if position and position < 0:
            raise ValueError(f"Position: {position} cannot be less than 0.")
        for task in tasks:
            if task.id in self._tasks:
                raise ValueError(f"TaskID '{task.id}' already in use!")
