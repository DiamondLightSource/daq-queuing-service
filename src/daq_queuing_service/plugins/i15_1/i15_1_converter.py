from typing import Any, Literal, get_args

from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    get_background_tiled_id,
)
from daq_queuing_service.task import Experiment, TaskWithPosition

SCAN_PLANS = Literal["centre_sample", "static_collection"]


class I151Converter(Converter):
    def _add_tiled_background_to_md(
        self, params: dict[str, Any], tiled_id: str, background: BackgroundInfo
    ):
        if metadata := params.get("metadata"):
            if tiled_backgrounds := metadata.get("tiled_backgrounds"):
                tiled_backgrounds[tiled_id] = background
            else:
                metadata["tiled_backgrounds"] = {tiled_id: background}
        else:
            params["metadata"] = {"tiled_backgrounds": {tiled_id: background}}

    def _get_required_backgrounds(self, experiment: Experiment) -> list[BackgroundInfo]:
        # TODO: Check if this should take a task request instead of experiment
        return [BackgroundInfo(bg_type="air", cobra=False, blower=False)]

    def _construct_background_task_request(
        self, background: BackgroundInfo, instrument_session: str
    ) -> TaskRequest:
        return TaskRequest(
            instrument_session=instrument_session,
            name="static_collection",
            params={"metadata": {"background": background}},
        )

    def _add_required_background_scans(
        self, tasks: list[TaskWithPosition], calls: list[BlueapiCall]
    ) -> list[BlueapiCall]:
        bg_task_requests: dict[str, list[TaskRequest]] = {}

        for task in tasks:
            instrument_session = task.experiment.instrument_session
            if not isinstance(task.experiment, Experiment) or not (
                backgrounds := self._get_required_backgrounds(task.experiment)
            ):
                break

            for background in backgrounds:
                blueapi_calls = [
                    call for call in calls if call.parent_task_id == task.id
                ]
                if tiled_id := get_background_tiled_id(background, instrument_session):
                    for call in filter(
                        lambda call: call.task_request.name in get_args(SCAN_PLANS),
                        blueapi_calls,
                    ):
                        call.task_request.params = dict(call.task_request.params)
                        self._add_tiled_background_to_md(
                            call.task_request.params, tiled_id, background
                        )

                else:
                    task_request = self._construct_background_task_request(
                        background, instrument_session
                    )
                    if not any(
                        task_request in task_requests
                        for task_requests in bg_task_requests.values()
                    ):
                        bg_task_requests.setdefault(task.id, []).append(task_request)

        new_call_list: list[BlueapiCall] = []

        for call in calls:
            if call.parent_task_id and (
                task_requests := bg_task_requests.pop(call.parent_task_id, None)
            ):
                new_call_list.extend(
                    [
                        BlueapiCall(
                            task_request=task_request,
                            parent_task_id=call.parent_task_id,
                        )
                        for task_request in task_requests
                    ]
                )
            new_call_list.append(call)

        return new_call_list

    def _construct_blueapi_tasks_from_experiment(
        self,
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

    def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:

        call_list: list[BlueapiCall] = []

        for task in queue:
            match task.experiment:
                case TaskRequest():
                    call_list.append(
                        BlueapiCall(
                            task_request=task.experiment, parent_task_id=task.id
                        )
                    )
                case Experiment():
                    call_list.extend(
                        [
                            BlueapiCall(task_request=b_api_task, parent_task_id=task.id)
                            for b_api_task in (
                                self._construct_blueapi_tasks_from_experiment(
                                    task.experiment
                                )
                            )
                        ]
                    )

        return self._add_required_background_scans(queue, call_list)
