from daq_queuing_service.plugins.construct_task_request import (
    construct_blueapi_call_list,
)
from daq_queuing_service.plugins.converter_utils import get_converter


def test_get_converter_returns_converter_from_path_and_name():
    converter = get_converter("construct_task_request", "construct_blueapi_call_list")
    assert converter == construct_blueapi_call_list
