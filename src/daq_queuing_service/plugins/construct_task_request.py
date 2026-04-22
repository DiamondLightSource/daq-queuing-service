from blueapi.service.model import TaskRequest

from daq_queuing_service.task import ExperimentDefinition


def construct_blueapi_task_request(
    experiment_definition: ExperimentDefinition,
) -> TaskRequest:
    return TaskRequest(
        name=experiment_definition.plan_name,
        params=experiment_definition.params,
        instrument_session=experiment_definition.instrument_session,
    )
