import importlib
from collections.abc import Callable

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.task import TaskWithPosition

Converter = Callable[
    [list[TaskWithPosition], list[TaskWithPosition], list[BlueapiCall]],
    list[BlueapiCall],
]


def get_converter(rel_path: str, converter_name: str) -> Converter:
    base_path = "daq_queuing_service.plugins."  # import relative to plugins directory
    module = importlib.import_module(base_path + rel_path)
    converter = getattr(module, converter_name)
    return converter
