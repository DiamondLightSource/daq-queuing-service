import importlib
from collections.abc import Callable

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import TaskWithPosition

Converter = Callable[
    [list[TaskWithPosition], list[TaskWithPosition], list[BlueapiCall]],
    list[BlueapiCall],
]


def get_converter(path: str, name: str) -> Converter:
    """Gets the converter function based on its path and name

    Args:
        path (str): Path to converter function. For example:
            "daq_queuing_service.plugins.construct_task_request"
        converter_name (str): Name of the converter function. For example:
            "construct_blueapi_call_list"

    Returns:
        Converter: Converter function
    """
    module = importlib.import_module(path)
    converter = getattr(module, name)
    return converter
