import importlib

from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import Experiment, Task, TaskWithPosition


class ConverterError(Exception): ...


class ValidateError(Exception): ...


class Converter:
    def __init__(self): ...

    def validate(self, experiments: list[TaskRequest | Experiment]) -> None:
        """This gets run on new experiments added to the queue through the api endpoint.
        Any error raised will be handled and cause the experiments to not be added to
        the queue.

        Args:
            experiments (list[TaskRequest  |  Experiment]): List of experiments or plans
            added to the queue.

        Raises:
            Exception: Any exception raised due to experiments failing validation
        """
        ...

    def pre_process(
        self,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[Task]:
        """This gets run whenever something changes in the queue, before
        `construct_blueapi_calls`. It is an opportunity to automatically modify the
        queue based on it's current state and history.

        For example, it could be used to add calibration scans before any experiments
        that require them. It could also be used to query another service such as ulims
        and add experiments to the queue.

        If not overridden, this method returns the queue as is with no modification.

        Args:
            queue (list[Task]): List of tasks currently in the queue
            history (list[TaskWithPosition]): List of completed tasks
            call_history (list[BlueapiCall]): List of completed blueapi calls

        Returns:
            list[Task]: The new list of tasks in the queue
        """
        return queue

    def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:
        """Converts the list of queued tasks into a list of blueapi calls. This is
        needed if the queue contains ulims experiments, as these must be mapped onto
        bluesky plans. If the queue contains TaskRequests, they can just be
        wrapped into BlueapiCall objects with no conversion, as this default
        implementation does.

        Args:
            queue (list[TaskWithPosition]): List of tasks in the queue
            history (list[TaskWithPosition]): List of completed tasks
            call_history (list[BlueapiCall]): List of completed blueapi calls

        Returns:
            list[BlueapiCall]: List of blueapi calls to execute.
        """

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
    converter_cls = getattr(module, name)
    converter = converter_cls()
    if not isinstance(converter, Converter):
        raise TypeError(f"Converter is not of type Converter, it is {type(converter)}")
    return converter
