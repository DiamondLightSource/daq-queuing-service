from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import ExperimentDefinition, TaskWithPosition


def construct_blueapi_task_request(
    experiment_definition: ExperimentDefinition,
) -> TaskRequest:
    return TaskRequest(
        name=experiment_definition.plan_name,
        params=experiment_definition.params,
        instrument_session=experiment_definition.instrument_session,
    )


def construct_blueapi_tasks_from_i15_1_experiment(
    experiment_definition: ExperimentDefinition,
) -> list[TaskRequest]:
    sample_name = experiment_definition.params["sampleName"]
    # Assume sample name is of form test_8_1 to load from position 8 on puck 1
    _, position, puck = sample_name.split("_")

    return [
        TaskRequest(
            name="robot_load",
            params={"puck": puck, "position": position},
            instrument_session=experiment_definition.instrument_session,
        ),
        TaskRequest(
            name="centre_sample",
            params={"start_z": -5, "end_z": 5, "steps": 20, "exposure_time": 0.01},
            instrument_session=experiment_definition.instrument_session,
        ),
        TaskRequest(
            name="robot_unload",
            params={},
            instrument_session=experiment_definition.instrument_session,
        ),
    ]


def construct_blueapi_call_list(
    queue: list[TaskWithPosition],
    history: list[TaskWithPosition],
    call_history: list[BlueapiCall],
) -> list[BlueapiCall]:

    call_list = [
        BlueapiCall(
            parent_task_id=task.id,
            task_request=construct_blueapi_task_request(task.experiment_definition),
        )
        for task in queue
    ]
    return call_list


def construct_i15_1_blueapi_call_list(
    queue: list[TaskWithPosition],
    history: list[TaskWithPosition],
    call_history: list[BlueapiCall],
) -> list[BlueapiCall]:

    call_list: list[BlueapiCall] = []

    for task in queue:
        # Remove plan name from here and UI in https://github.com/DiamondLightSource/daq-queuing-service/issues/51
        if task.experiment_definition.plan_name == "run_full_collection":
            call_list.extend(
                [
                    BlueapiCall(task_request=blueapi_task, parent_task_id=task.id)
                    for blueapi_task in construct_blueapi_tasks_from_i15_1_experiment(
                        task.experiment_definition
                    )
                ]
            )

    return call_list
