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

    async def complete_task(self, task_id: TaskID, blueapi_info: str):
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
            self.history.append(task_id)
            self.condition.notify_all()

    async def get_task_by_id(self, task_id: str) -> Task | None:
        async with self.lock:
            return self._tasks.get(task_id)

    async def get_task_by_position(self, position: int) -> Task | None:
        async with self.lock:
            return self._tasks[self.queue[position]] if position < self.length else None

    async def get_queue(self) -> list[str]:
        async with self.lock:
            tasks = [self._tasks[task_id] for task_id in self.queue]
        return [task.model_dump_json() for task in tasks]

    async def get_history(self) -> list[str]:
        async with self.lock:
            tasks = [self._tasks[task_id] for task_id in self.history]
        return [task.model_dump_json() for task in tasks]

    async def get_tasks(self) -> list[str]:
        async with self.lock:
            tasks = [self._tasks[task_id] for task_id in self.history] + [
                self._tasks[task_id] for task_id in self.queue
            ]
        return [task.model_dump_json() for task in tasks]

    async def add_tasks(self, tasks: list[Task], position: int | None = None) -> None:
        async with self.condition:
            self._verify_new_tasks(tasks, position)
            if position is not None:
                position = self._validate_position(position)
            self._add_tasks(tasks, position)
            self.condition.notify_all()

    async def move_task(self, task_id: TaskID, position: int):
        async with self.condition:
            position = self._validate_position(position)
            task_ids = self._remove_tasks_from_queue([task_id])
            self.queue[position:position] = task_ids
            self.condition.notify_all()

    def _validate_position(self, position: int) -> int:
        if position < 0:
            raise ValueError(f"Position must be >= 0, got {position}")
        if (
            self.length
            and position == 0
            and self._tasks[self.queue[0]].status != Status.WAITING
        ):
            return 1
        return position

    async def remove_tasks(self, task_ids: list[TaskID]) -> list[Task]:
        async with self.condition:
            task_ids = self._remove_tasks_from_queue(task_ids)
            tasks = self._remove_tasks_from_registry(task_ids)
            self.condition.notify_all()
            return tasks

    async def clear_history(self):
        async with self.condition:
            for task_id in self.history:
                self._tasks.pop(task_id)
            self.history.clear()
            self.condition.notify_all()

    def _add_tasks(self, tasks: list[Task], position: int | None) -> None:
        task_ids = [task.id for task in tasks]
        if position is None:
            self.queue.extend(task_ids)
        else:
            self.queue[position:position] = task_ids
        for task in tasks:
            self._tasks[task.id] = task

    def _remove_tasks_from_queue(self, task_ids: list[TaskID]) -> list[TaskID]:
        #  Only removes tasks in the queue (not history)
        def should_be_removed(task_id: TaskID):
            return (
                task_id in self.queue
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        self.queue = [task_id for task_id in self.queue if task_id not in removed_ids]

        return removed_ids

    def _remove_tasks_from_registry(self, task_ids: list[TaskID]) -> list[Task]:
        def should_be_removed(task_id: TaskID):
            return (
                task_id in self._tasks
                and self._tasks[task_id].status != Status.IN_PROGRESS
            )

        removed_ids = [task_id for task_id in task_ids if should_be_removed(task_id)]
        removed = [self._tasks[task_id] for task_id in removed_ids]
        self._tasks = {
            task_id: task
            for task_id, task in self._tasks.items()
            if task.id not in removed_ids
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
