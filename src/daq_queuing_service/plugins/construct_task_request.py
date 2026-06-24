from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import Experiment, TaskWithPosition


def _construct_blueapi_task_request(
    experiment: Experiment | TaskRequest,
) -> TaskRequest:
    match experiment:
        case TaskRequest():
            return experiment
        case Experiment():
            return TaskRequest(
                instrument_session=experiment.instrument_session,
                name=experiment.experiment_definition.name,
                params=experiment.experiment_definition.data,
            )


def construct_blueapi_call_list(
    queue: list[TaskWithPosition],
    history: list[TaskWithPosition],
    call_history: list[BlueapiCall],
) -> list[BlueapiCall]:

    call_list = [
        BlueapiCall(
            parent_task_id=task.id,
            task_request=_construct_blueapi_task_request(task.experiment),
        )
        for task in queue
    ]
    return call_list
