from functools import cached_property
from typing import Any

from blueapi.service.model import TaskRequest
from tiled.client.container import Container as TiledContainer

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.backgrounds import (
    BackgroundInfo,
    TiledBackground,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    BACKGROUND_SCAN,
    get_tiled_background,
    get_tiled_client,
)
from daq_queuing_service.task_queue.task import (
    Container,
    ContainerPosition,
    Experiment,
    ExperimentDefinition,
    Sample,
    Task,
    TaskWithPosition,
)


class I151Converter(Converter):
    def __init__(self):
        self._tiled_backgrounds: dict[str, list[TiledBackground]] = {}

    @cached_property
    def _tiled_client(self) -> TiledContainer:
        return get_tiled_client()

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
                                    task.experiment, task.id
                                )
                            )
                        ]
                    )
        return call_list

    def _construct_blueapi_tasks_from_experiment(
        self, experiment: Experiment, task_id: str
    ) -> list[TaskRequest]:
        LOGGER.debug(f"Converting to blueapi calls, experiment = {experiment}")
        position = experiment.sample.positionInContainer.position
        puck = experiment.sample.container.positionInParent.position

        metadata: dict[str, Any] = {
            "sample": experiment.sample,
            "experiment_definition": experiment.experiment_definition,
        }
        if tiled_backgrounds := self._tiled_backgrounds.get(task_id):
            metadata["tiled_backgrounds"] = tiled_backgrounds

        # Assume collections with lists of temperatures are blowers, see
        # https://github.com/DiamondLightSource/crystallography-bluesky/issues/125
        if "list_of_temperatures" in experiment.experiment_definition.data.keys():
            data_collection = TaskRequest(
                name="blower_collection",
                params={
                    "time_per_collection": experiment.experiment_definition.data[
                        "time_per_pdf"
                    ],
                    "exposure_time_per_frame": 0.1,
                    "ramp_rate_c_per_min": experiment.experiment_definition.data[
                        "ramp_rate"
                    ],
                    "settle_time": experiment.experiment_definition.data["settle_time"],
                    "temperatures_celsius": experiment.experiment_definition.data[
                        "list_of_temperatures"
                    ],
                    "metadata": metadata,
                },
                instrument_session=experiment.instrument_session,
            )
        else:
            data_collection = TaskRequest(
                name="data_collection",
                params={
                    "full_collection_time": experiment.experiment_definition.data[
                        "time_per_pdf"
                    ],
                    "exposure_time_per_frame": 0.1,
                    "metadata": metadata,
                },
                instrument_session=experiment.instrument_session,
            )

        # For air calibration scans, we need to not to robot load/unload.
        # https://github.com/DiamondLightSource/daq-queuing-service/issues/83
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
            data_collection,
            TaskRequest(
                name="robot_unload",
                params={},
                instrument_session=experiment.instrument_session,
            ),
        ]

    def _add_required_background_scans(self, tasks: list[Task]) -> list[Task]:
        """Adds background scan tasks to the queue. Backgrounds will be added directly
        in front of the first task in the queue that requires them.

        Args:
            tasks (list[Task]): Current list of tasks

        Returns:
            list[Task]: New list of tasks including backgrounds
        """
        LOGGER.info("Adding required background scans")
        self._tiled_backgrounds = {task.id: [] for task in tasks}

        # This can be made more robust https://github.com/DiamondLightSource/daq-queuing-service/issues/80
        new_tasks: list[Task] = []

        pdf_times = [
            task.experiment.experiment_definition.data["time_per_pdf"]
            for task in tasks
            if isinstance(task.experiment, Experiment)
            and not task.experiment.name == BACKGROUND_SCAN
        ]

        max_time_per_pdf = max(pdf_times) if pdf_times else 10

        for task in tasks:
            experiment = task.experiment
            if (
                isinstance(experiment, Experiment)
                and experiment.name != BACKGROUND_SCAN
            ):
                instrument_session = experiment.instrument_session
                backgrounds = self._get_required_backgrounds(
                    experiment, max_time_per_pdf
                )

                for background in backgrounds:
                    if tiled_background := get_tiled_background(
                        self._tiled_client,
                        background,
                        instrument_session,
                    ):
                        self._tiled_backgrounds[task.id].append(tiled_background)

                    else:
                        bg_experiment = self._construct_background_experiment(
                            background, instrument_session
                        )
                        new_tasks.append(Task(experiment=bg_experiment))

            new_tasks.append(task)
        return self._remove_repeated_backgrounds(new_tasks)

    def _remove_repeated_backgrounds(self, tasks: list[Task]) -> list[Task]:
        LOGGER.info("Removing repeated background scans")
        new_tasks: list[Task] = []
        queued_background_experiments: list[Experiment] = []

        for task in tasks:
            if task.experiment.name != BACKGROUND_SCAN:
                new_tasks.append(task)
            elif task.experiment not in queued_background_experiments:
                assert isinstance(task.experiment, Experiment)
                queued_background_experiments.append(task.experiment)
                new_tasks.append(task)
            else:
                LOGGER.debug(f"Removing repeated background scan: {task.experiment}")
        return new_tasks

    def _get_required_backgrounds(
        self, experiment: Experiment, time_per_pdf: int
    ) -> list[BackgroundInfo]:
        # This should be fleshed out https://github.com/DiamondLightSource/daq-queuing-service/issues/79
        # And we should instead do the following to work out pdf_times for backgrounds
        # https://github.com/DiamondLightSource/daq-queuing-service/issues/80
        return [BackgroundInfo(bg_type="fq", time_per_pdf=time_per_pdf)]

    def _construct_background_experiment(
        self, background: BackgroundInfo, instrument_session: str
    ) -> Experiment:
        LOGGER.debug(f"Constructing experiment for background: {background}")
        container_position = ContainerPosition(position=1)
        return Experiment(
            name=BACKGROUND_SCAN,
            instrument_session=instrument_session,
            # Need to get sample info for test samples (air, empty capillary etc)
            sample=Sample(
                name="fq Background Sample",
                id="",
                data={},
                container=Container(id="", positionInParent=container_position),
                positionInContainer=container_position,
            ),
            experiment_definition=ExperimentDefinition(
                name=BACKGROUND_SCAN,
                id="",
                data={
                    "background": background,
                    "time_per_pdf": background.time_per_pdf,
                },
            ),
        )
