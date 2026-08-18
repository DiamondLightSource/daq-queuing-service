from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.task import Experiment, TaskWithPosition


class I151Converter(Converter):
    def _construct_blueapi_tasks_from_experiment(
        self,
        experiment: Experiment,
    ) -> list[TaskRequest]:
        sample_name = experiment.sample.name
        # Assume sample name is of form test_8_1 to load from position 8 on puck 1
        _, position, puck = sample_name.split("_")

        return [
            TaskRequest(
                name="robot_load",
                params={"puck": puck, "position": position},
                instrument_session=experiment.instrument_session,
            ),
            TaskRequest(
                name="centre_sample",
                params={
                    "start_z": -20,
                    "end_z": 0,
                    "steps": 20,
                    "exposure_time": 0.01,
                    "metadata": {
                        "sample": experiment.sample,
                        "experiment_definition": experiment.experiment_definition,
                    },
                },
                instrument_session=experiment.instrument_session,
            ),
            TaskRequest(
                name="data_collection",
                params={
                    "full_collection_time": experiment.experiment_definition.data[
                        "time_per_pdf"
                    ],
                    "exposure_time_per_frame": 0.01,
                    "metadata": {
                        "sample": experiment.sample,
                        "experiment_definition": experiment.experiment_definition,
                    },
                },
                instrument_session=experiment.instrument_session,
            ),
            TaskRequest(
                name="robot_unload",
                params={},
                instrument_session=experiment.instrument_session,
            ),
        ]

    def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:

        call_list: list[BlueapiCall] = []

        for task in queue:
            match task.experiment:
                case TaskRequest():
                    call_list.append(
                        BlueapiCall(
                            task_request=task.experiment, parent_task_id=task.id
                        )
                    )
                case Experiment():
                    call_list.extend(
                        [
                            BlueapiCall(task_request=b_api_task, parent_task_id=task.id)
                            for b_api_task in (
                                self._construct_blueapi_tasks_from_experiment(
                                    task.experiment
                                )
                            )
                        ]
                    )

        return call_list
