from blueapi.service.model import TaskRequest

from daq_queuing_service.client.client import QueueClient
from daq_queuing_service.task_queue.queue import QueueState

# Example of how to add tasks to the queue using the Python client
# Need a local queue running on port 8001 for this to work


def main():
    client = QueueClient("http://127.0.0.1:8001")

    # Pause the queue
    client.update_queue_state(QueueState(paused=True))

    # Clear everything in the queue
    client.cancel_all_tasks()

    # Add 10 sleep plans to the queue
    client.add_tasks_to_queue(
        [
            TaskRequest(
                name="sleep", instrument_session="cm44163-3", params={"time": 1}
            )
            for _ in range(10)
        ],
    )

    # Add a longer sleep to the start of the queue
    client.add_tasks_to_queue(
        [TaskRequest(name="sleep", instrument_session="cm44163-3", params={"time": 5})],
        position=0,
    )

    # Print the current tasks in the queue
    tasks = client.get_queued_tasks()
    for task in tasks:
        assert isinstance(task.experiment, TaskRequest)
        print(
            f"Task {task.position}: {task.experiment.name} for "
            + f"{task.experiment.params['time']} seconds"
        )


if __name__ == "__main__":
    main()
