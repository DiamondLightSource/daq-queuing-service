from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import Experiment, TaskWithPosition


def construct_blueapi_tasks_from_i15_1_experiment(
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
            params={"start_z": -5, "end_z": 5, "steps": 20, "exposure_time": 0.01},
            instrument_session=experiment.instrument_session,
        ),
        TaskRequest(
            name="robot_unload",
            params={},
            instrument_session=experiment.instrument_session,
        ),
    ]


def construct_i15_1_blueapi_call_list(
    queue: list[TaskWithPosition],
    history: list[TaskWithPosition],
    call_history: list[BlueapiCall],
) -> list[BlueapiCall]:

    call_list: list[BlueapiCall] = []

    for task in queue:
        if isinstance(task.experiment, TaskRequest):
            call_list.append(
                BlueapiCall(task_request=task.experiment, parent_task_id=task.id)
            )
        # Remove plan name from here and UI in https://github.com/DiamondLightSource/daq-queuing-service/issues/51
        elif task.experiment.experiment_definition.name == "run_full_collection":
            call_list.extend(
                [
                    BlueapiCall(task_request=blueapi_task, parent_task_id=task.id)
                    for blueapi_task in construct_blueapi_tasks_from_i15_1_experiment(
                        task.experiment
                    )
                ]
            )

    return call_list
