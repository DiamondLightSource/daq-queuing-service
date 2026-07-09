from collections.abc import Mapping
from typing import Any, Literal, get_args

from blueapi.service.model import TaskRequest
from pydantic import BaseModel

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    get_background_tiled_id,
)
from daq_queuing_service.task import Experiment, TaskWithPosition

BACKGROUND = Literal["air", "capillary_1", "capillary_2"]
SCAN_PLANS = Literal["centre_sample", "static_collection"]


class BackgroundInfo(BaseModel):
    bg_type: BACKGROUND
    cobra: bool
    blower: bool

    def add_tiled_id(self, tiled_id: str) -> "TiledBackground":
        return TiledBackground(
            bg_type=self.bg_type,
            cobra=self.cobra,
            blower=self.blower,
            tiled_id=tiled_id,
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str


def add_tiled_background_to_md(
    params: Mapping[str, Any], tiled_id: str, background: BackgroundInfo
):
    if metadata := params.get("metadata"):
        if tiled_backgrounds := params.get("tiled_backgrounds"):
            tiled_backgrounds.update({tiled_id: background})
        else:
            metadata.update({"tiled_backgrounds": {tiled_id: background}})
    else:
        params = {
            **params,
            "metadata": {"background_tiled_id": tiled_id},
        }


def get_required_backgrounds(experiment: Experiment) -> list[BackgroundInfo]:
    # TODO: Check if this should take a task request instead of experiment
    return [BackgroundInfo(bg_type="air", cobra=False, blower=False)]


def construct_background_task_request(
    background: BackgroundInfo, instrument_session: str
) -> TaskRequest:
    return TaskRequest(
        instrument_session=instrument_session,
        name="static_collection",
        params={"metadata": {"background": background}},
    )


def add_required_background_scans(
    tasks: list[TaskWithPosition], calls: list[BlueapiCall]
) -> list[BlueapiCall]:
    bg_task_requests: dict[str, TaskRequest] = {}

    for task in tasks:
        instrument_session = task.experiment.instrument_session
        if not isinstance(task.experiment, Experiment) or not (
            backgrounds := get_required_backgrounds(task.experiment)
        ):
            break

        for background in backgrounds:
            blueapi_calls = [call for call in calls if call.parent_task_id == task.id]
            if tiled_id := get_background_tiled_id(background, instrument_session):
                for call in filter(
                    lambda call: call.task_request.name in get_args(SCAN_PLANS),
                    blueapi_calls,
                ):
                    add_tiled_background_to_md(
                        call.task_request.params, tiled_id, background
                    )

            else:
                task_request = construct_background_task_request(
                    background, instrument_session
                )
                if task_request not in bg_task_requests.values():
                    bg_task_requests[task.id] = task_request

    new_call_list: list[BlueapiCall] = []

    for call in calls:
        if call.parent_task_id and (
            task_request := bg_task_requests.get(call.parent_task_id)
        ):
            new_call_list.append(BlueapiCall(task_request=task_request))
        new_call_list.append(call)

    return new_call_list


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
            params={
                "start_z": -20,
                "end_z": 0,
                "steps": 20,
                "exposure_time": 0.01,
                "metadata": {
                    "sample": experiment.sample,
                    "experiment_definition": experiment.experiment_definition,
                },
            },
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
        match task.experiment:
            case TaskRequest():
                call_list.append(
                    BlueapiCall(task_request=task.experiment, parent_task_id=task.id)
                )
            case Experiment():
                call_list.extend(
                    [
                        BlueapiCall(task_request=b_api_task, parent_task_id=task.id)
                        for b_api_task in construct_blueapi_tasks_from_i15_1_experiment(
                            task.experiment
                        )
                    ]
                )

    return add_required_background_scans(queue, call_list)
