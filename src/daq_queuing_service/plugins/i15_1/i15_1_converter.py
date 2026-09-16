from enum import StrEnum
from functools import cached_property
from typing import Any

from blueapi.service.model import TaskRequest
from tiled.client.container import Container as TiledContainer

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.backgrounds import (
    AuxiliaryScanType,
    BackgroundInfo,
    TiledBackground,
    is_auxiliary_str,
)
from daq_queuing_service.plugins.i15_1.standards import (
    STANDARDS_PUCK_PLACEMENT,
    StandardsPin,
    StandardsPuck,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    get_suitable_tiled_background,
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


class ScanType(StrEnum):
    DATA_COLLECTION = "Data Collection"
    CENTRING = "Centring"
    AIR = AuxiliaryScanType.AIR
    EMPTY_CAPILLARY = AuxiliaryScanType.EMPTY_CAPILLARY
    STANDARD_SAMPLE = AuxiliaryScanType.STANDARD_SAMPLE


def _filter_backgrounds(tasks: list[Task]) -> list[tuple[int, BackgroundInfo]]:
    return [
        (i, BackgroundInfo.from_experiment(task.experiment))
        for i, task in enumerate(tasks)
        if isinstance(task.experiment, Experiment)
        and is_auxiliary_str(task.experiment.name)
    ]


class I151Converter(Converter):
    def __init__(self):
        # First key is the ID of the task using the background
        # Second key is the tiled ID of the background
        self._tiled_backgrounds: dict[
            str, dict[AuxiliaryScanType, TiledBackground]
        ] = {}
        self._standards_puck = StandardsPuck()

    @cached_property
    def _tiled_client(self) -> TiledContainer:
        return get_tiled_client()

    def pre_process(
        self,
        current_task: TaskWithPosition | None,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[Task]:
        return self._add_required_background_scans(current_task, queue)

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

        match experiment.name:
            case ScanType.AIR:
                scan_type = ScanType.AIR
            case ScanType.EMPTY_CAPILLARY:
                scan_type = ScanType.EMPTY_CAPILLARY
            case ScanType.STANDARD_SAMPLE:
                scan_type = ScanType.STANDARD_SAMPLE
            case _:
                scan_type = ScanType.DATA_COLLECTION

        collection_metadata: dict[str, Any] = {
            "sample": experiment.sample,
            "experiment_definition": experiment.experiment_definition,
            "scan_type": scan_type,
        }
        if tiled_backgrounds := self._tiled_backgrounds.get(task_id):
            collection_metadata["auxiliary_scans"] = tiled_backgrounds

        # Assume collections with lists of temperatures are blowers, see
        # https://github.com/DiamondLightSource/crystallography-bluesky/issues/125
        time_per_pdf = experiment.experiment_definition.data["time_per_pdf"]

        if "list_of_temperatures" in experiment.experiment_definition.data.keys():
            data_collection = TaskRequest(
                name="blower_collection",
                params={
                    "time_per_collection": time_per_pdf,
                    "exposure_time_per_frame": 0.1,
                    "ramp_rate_c_per_min": experiment.experiment_definition.data[
                        "ramp_rate"
                    ],
                    "settle_time": experiment.experiment_definition.data["settle_time"],
                    "temperatures_celsius": experiment.experiment_definition.data[
                        "list_of_temperatures"
                    ],
                    "metadata": collection_metadata,
                },
                instrument_session=experiment.instrument_session,
            )
        else:
            data_collection = TaskRequest(
                name="data_collection",
                params={
                    "full_collection_time": time_per_pdf,
                    "exposure_time_per_frame": 0.1,
                    "metadata": collection_metadata,
                },
                instrument_session=experiment.instrument_session,
            )

        if experiment.sample is None:
            return [data_collection]  # Air background

        position = experiment.sample.positionInContainer.position
        puck = experiment.sample.container.positionInParent.position

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
                        "scan_type": ScanType.CENTRING,
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

    def _add_required_background_scans(
        self, current_task: TaskWithPosition | None, tasks: list[Task]
    ) -> list[Task]:
        """Adds background scan tasks to the queue. Backgrounds will be added directly
        in front of the first task in the queue that requires them.

        Args:
            current_task (TaskWithPosition | None): Current running task, if one exists.
            tasks (list[Task]): Current list of tasks

        Returns:
            list[Task]: New list of tasks including backgrounds
        """
        LOGGER.info("Adding required background scans")
        self._tiled_backgrounds = {task.id: {} for task in tasks}

        new_tasks: list[Task] = []

        for task in tasks:
            experiment = task.experiment
            if isinstance(experiment, Experiment) and not is_auxiliary_str(
                experiment.name
            ):
                instrument_session = experiment.instrument_session

                required_backgrounds = self._get_required_backgrounds(experiment)

                for background in required_backgrounds:
                    new_tasks = self._ensure_background_in_queue_or_tiled(
                        background, current_task, new_tasks, task.id, instrument_session
                    )

            new_tasks.append(task)
        return new_tasks

    def _ensure_background_in_queue_or_tiled(
        self,
        required_background: BackgroundInfo,
        current_task: TaskWithPosition | None,
        new_tasks: list[Task],
        task_id: str,
        instrument_session: str,
    ):
        if (
            current_task
            and isinstance(current_task.experiment, Experiment)
            and is_auxiliary_str(current_task.experiment.name)
            and BackgroundInfo.from_experiment(current_task.experiment).is_suitable(
                required_background
            )
        ):
            return new_tasks

        queued_backgrounds = _filter_backgrounds(new_tasks)
        if any(
            queued_background.is_suitable(required_background)
            for _, queued_background in queued_backgrounds
        ):
            return new_tasks

        if tiled_background := get_suitable_tiled_background(
            self._tiled_client, required_background
        ):
            self._tiled_backgrounds[task_id][tiled_background.kind] = tiled_background
            return new_tasks

        LOGGER.info(
            f"No existing suitable backgrounds found for {required_background}, "
            + "modifying or adding one"
        )
        return self._add_or_replace_background(
            required_background,
            new_tasks,
            queued_backgrounds,
            instrument_session,
        )

    def _add_or_replace_background(
        self,
        required_background: BackgroundInfo,
        new_tasks: list[Task],
        queued_backgrounds: list[tuple[int, BackgroundInfo]],
        instrument_session: str,
    ) -> list[Task]:
        combined_background = None
        index = None

        for i, background in queued_backgrounds:
            if combined_background := background.attempt_to_combine_with(
                required_background
            ):
                index = i
                break

        bg_experiment = self._construct_background_experiment(
            combined_background or required_background, instrument_session
        )
        if index is None:
            new_tasks.append(Task(experiment=bg_experiment))
        else:
            new_tasks[index] = Task(experiment=bg_experiment)
        return new_tasks

    def _get_required_backgrounds(self, experiment: Experiment) -> list[BackgroundInfo]:
        # This should be fleshed out https://github.com/DiamondLightSource/daq-queuing-service/issues/79
        time_per_pdf = experiment.experiment_definition.data["time_per_pdf"]
        assert experiment.sample
        return [
            BackgroundInfo(
                instrument_session=experiment.instrument_session,
                pin=None,
                time_per_pdf=time_per_pdf,
            ),
            BackgroundInfo(
                instrument_session=experiment.instrument_session,
                pin=StandardsPin(
                    capillary=experiment.sample.data["capillary"], contents=None
                ),
                time_per_pdf=time_per_pdf,
            ),
            BackgroundInfo(
                instrument_session=experiment.instrument_session,
                pin=StandardsPin(
                    capillary=experiment.sample.data["capillary"], contents="Silicon"
                ),
                time_per_pdf=time_per_pdf,
            ),
        ]

    def _construct_background_experiment(
        self, background: BackgroundInfo, instrument_session: str
    ) -> Experiment:
        LOGGER.debug(f"Constructing experiment for background: {background}")

        return Experiment(
            name=background.kind,
            instrument_session=instrument_session,
            # Need to get sample info for test samples (air, empty capillary etc)
            sample=self._construct_background_sample(background.pin),
            experiment_definition=ExperimentDefinition(
                name=f"Auxiliary {background.kind} Scan",
                id="",
                data={"time_per_pdf": background.time_per_pdf},
            ),
        )

    def _construct_background_sample(self, pin: StandardsPin | None):
        if pin is None:
            return

        data = {"capillary": pin.capillary, "composition": pin.contents}
        puck = Container(
            id="",
            positionInParent=ContainerPosition(position=STANDARDS_PUCK_PLACEMENT),
        )
        position = ContainerPosition(position=self._standards_puck.get_pin_number(pin))

        if pin.contents is None:
            name = f"Empty {pin.capillary}"
        else:
            name = f"{pin.contents} {pin.capillary}"

        return Sample(
            name=name,
            data=data,
            id="",
            container=puck,
            positionInContainer=position,
        )
