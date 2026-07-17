import importlib

from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import Experiment, Task, TaskWithPosition


class Converter:
    def __init__(self): ...

    def pre_process(
        self,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[Task]:
        return queue

    def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:

        call_list = [
            BlueapiCall(
                parent_task_id=task.id,
                task_request=self._construct_blueapi_task_request(task.experiment),
            )
            for task in queue
        ]
        return call_list

    def _construct_blueapi_task_request(
        self,
        experiment: Experiment | TaskRequest,
    ) -> TaskRequest:
        match experiment:
            case TaskRequest():
                return experiment
            case Experiment() as experiment:
                raise NotImplementedError(
                    f"No conversion implemented for {type(experiment)}. "
                    + "Try using a different converter"
                )


def get_converter(path: str, name: str) -> Converter:
    """Instantiates a converter based on a path and class name

    Args:
        path (str): Path to converter class. For example:
            "daq_queuing_service.plugins.converter"
        converter_name (str): Name of the converter class. For example:
            "Converter"

    Returns:
        Converter: Converter instance
    """
    module = importlib.import_module(path)
    converter_cls: type[Converter] = getattr(module, name)
    return converter_cls()
