from blueapi.service.model import TaskRequest

from daq_queuing_service.client.client import QueueClient


def add_stuff_to_queue(port: int = 8001):
    client = QueueClient(f"http://127.0.0.1:{port}")

    client.add_tasks_to_queue(
        [
            TaskRequest(
                name="sleep", instrument_session="cm44613-3", params={"time": 10}
            )
            for _ in range(10)
        ]
    )


if __name__ == "__main__":
    add_stuff_to_queue()
