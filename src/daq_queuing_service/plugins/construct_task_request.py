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


def construct_blueapi_call_list(
    queue: list[TaskWithPosition], history: list[TaskWithPosition]
) -> list[BlueapiCall]:

    call_list = [
        BlueapiCall(
            parent_task_id=task.id,
            task_request=construct_blueapi_task_request(task.experiment_definition),
        )
        for task in queue
    ]
    return call_list
