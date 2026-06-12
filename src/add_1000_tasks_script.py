from daq_queuing_service.client.client import QueueClient
from daq_queuing_service.task import ExperimentDefinition


def main():
    client = QueueClient("https://i15-1-daq-queue.diamond.ac.uk/")
    n_scans = 1000

    # client.cancel_all_tasks()

    client.add_tasks_to_queue(
        [
            ExperimentDefinition(
                plan_name="static_collect_and_trigger_analysis",
                sample_id=f"COLLECTION {i}",
                params={"frames": 200, "exposure_time": 0.05},
                instrument_session="cm44163-3",
            )
            for i in range(n_scans)
        ],
        validate_with_blueapi=False,
    )[0]


if __name__ == "__main__":
    main()
