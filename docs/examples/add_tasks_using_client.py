from blueapi.service.model import TaskRequest

from daq_queuing_service.client.client import QueueClient

# Example of how to add tasks to the queue using the Python client
# Need a local queue running on port 8001 for this to work


def main():
    client = QueueClient("http://127.0.0.1:8001")
    n_tasks = 10

    client.add_tasks_to_queue(
        [
            TaskRequest(
                name="sleep", instrument_session="cm44163-3", params={"time": 10}
            )
            for _ in range(n_tasks)
        ],
    )


if __name__ == "__main__":
    main()
