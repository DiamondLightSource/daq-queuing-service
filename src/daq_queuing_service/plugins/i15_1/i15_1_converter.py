from typing import Any, Literal

from blueapi.service.model import TaskRequest

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.backgrounds import BackgroundInfo
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    get_background_tiled_id,
)
from daq_queuing_service.task import (
    Experiment,
    ExperimentDefinition,
    Sample,
    Task,
    TaskWithPosition,
)

SCAN_PLANS = Literal["centre_sample", "static_collection"]


class I151Converter(Converter):
    def pre_process(
        self,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[Task]:
        return self._add_required_background_scans(queue)

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
        return call_list

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
                        # This will include tiled background scan info
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

    def _add_required_background_scans(self, tasks: list[Task]) -> list[Task]:
        new_tasks: list[Task] = []

        for task in tasks:
            experiment = task.experiment
            if isinstance(experiment, Experiment):
                instrument_session = experiment.instrument_session
                backgrounds = self._get_required_backgrounds(experiment)

                for background in backgrounds:
                    if tiled_id := get_background_tiled_id(
                        background, instrument_session
                    ):
                        self._add_tiled_background_to_md(
                            experiment.experiment_definition.data, tiled_id, background
                        )

                    else:
                        bg_experiment = self._construct_background_experiment(
                            background, instrument_session
                        )
                        if bg_experiment not in [
                            t.experiment for t in tasks + new_tasks
                        ]:
                            new_tasks.append(Task(experiment=bg_experiment))

            new_tasks.append(task)
        return new_tasks

    def _get_required_backgrounds(self, experiment: Experiment) -> list[BackgroundInfo]:
        return [BackgroundInfo(bg_type="air", cobra=False, blower=False)]

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

    def _construct_background_experiment(
        self, background: BackgroundInfo, instrument_session: str
    ) -> Experiment:
        return Experiment(
            name="background",
            instrument_session=instrument_session,
            # Need to get sample info for test samples (air, empty capillary etc)
            sample=Sample(name="air", id="", data={}),
            experiment_definition=ExperimentDefinition(
                name="background_scan", id="", data={"background": background}
            ),
        )
