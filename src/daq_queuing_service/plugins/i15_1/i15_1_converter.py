from enum import StrEnum
from functools import cached_property
from typing import Any

from blueapi.service.model import TaskRequest
from tiled.client.container import Container as TiledContainer

from daq_queuing_service.blueapi_interaction.blueapi_call import BlueapiCall
from daq_queuing_service.log import LOGGER
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.auxiliary import (
    AuxiliaryScan,
    AuxiliaryScanType,
    TiledAuxiliary,
    is_auxiliary_str,
)
from daq_queuing_service.plugins.i15_1.standards import (
    STANDARDS_PUCK_PLACEMENT,
    StandardsPin,
    StandardsPuck,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import (
    get_suitable_tiled_scan,
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


def _filter_auxiliary_scans(tasks: list[Task]) -> list[tuple[int, AuxiliaryScan]]:
    return [
        (i, AuxiliaryScan.from_experiment(task.experiment))
        for i, task in enumerate(tasks)
        if isinstance(task.experiment, Experiment)
        and is_auxiliary_str(task.experiment.name)
    ]


class I151Converter(Converter):
    def __init__(self):
        # First key is the ID of the task using the auxiliary scan
        # Second key is the scan type of the auxiliary scan
        self._tiled_auxiliary_scans: dict[
            str, dict[AuxiliaryScanType, TiledAuxiliary]
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
        return self._add_required_auxiliary_scans(current_task, queue)

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
        if tiled_auxiliary_scans := self._tiled_auxiliary_scans.get(task_id):
            collection_metadata["auxiliary_scans"] = tiled_auxiliary_scans

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
            return [data_collection]  # Air scan

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

    def _add_required_auxiliary_scans(
        self, current_task: TaskWithPosition | None, tasks: list[Task]
    ) -> list[Task]:
        """Adds auxiliary scan tasks to the queue. They will be added directly
        in front of the first task in the queue that requires them.

        Args:
            current_task (TaskWithPosition | None): Current running task, if one exists.
            tasks (list[Task]): Current list of tasks

        Returns:
            list[Task]: New list of tasks including auxiliary scans
        """
        LOGGER.info("Adding required auxiliary scans")
        self._tiled_auxiliary_scans = {task.id: {} for task in tasks}

        new_tasks: list[Task] = []

        for task in tasks:
            experiment = task.experiment
            if isinstance(experiment, Experiment) and not is_auxiliary_str(
                experiment.name
            ):
                instrument_session = experiment.instrument_session

                required_auxiliary_scans = self._get_required_auxiliary_scans(
                    experiment
                )

                for required_auxiliary in required_auxiliary_scans:
                    new_tasks = self._ensure_auxiliary_in_queue_or_tiled(
                        required_auxiliary,
                        current_task,
                        new_tasks,
                        task.id,
                        instrument_session,
                    )

            new_tasks.append(task)
        return new_tasks

    def _ensure_auxiliary_in_queue_or_tiled(
        self,
        required_auxiliary: AuxiliaryScan,
        current_task: TaskWithPosition | None,
        new_tasks: list[Task],
        task_id: str,
        instrument_session: str,
    ):
        if (
            current_task
            and isinstance(current_task.experiment, Experiment)
            and is_auxiliary_str(current_task.experiment.name)
            and AuxiliaryScan.from_experiment(current_task.experiment).is_suitable(
                required_auxiliary
            )
        ):
            return new_tasks

        queued_auxiliary_scans = _filter_auxiliary_scans(new_tasks)
        if any(
            queued_auxiliary.is_suitable(required_auxiliary)
            for _, queued_auxiliary in queued_auxiliary_scans
        ):
            return new_tasks

        if tiled_scan := get_suitable_tiled_scan(
            self._tiled_client, required_auxiliary
        ):
            self._tiled_auxiliary_scans[task_id][tiled_scan.kind] = tiled_scan
            return new_tasks

        LOGGER.info(
            f"No existing suitable auxiliary scans found for {required_auxiliary}, "
            + "modifying or adding one"
        )
        return self._add_or_replace_auxiliary_scan(
            required_auxiliary,
            new_tasks,
            queued_auxiliary_scans,
            instrument_session,
        )

    def _add_or_replace_auxiliary_scan(
        self,
        required_auxiliary: AuxiliaryScan,
        new_tasks: list[Task],
        queued_auxiliary_scans: list[tuple[int, AuxiliaryScan]],
        instrument_session: str,
    ) -> list[Task]:
        combined_auxiliary_scan = None
        index = None

        for i, auxiliary_scan in queued_auxiliary_scans:
            if combined_auxiliary_scan := auxiliary_scan.attempt_to_combine_with(
                required_auxiliary
            ):
                index = i
                break

        aux_experiment = self._construct_auxiliary_experiment(
            combined_auxiliary_scan or required_auxiliary, instrument_session
        )
        if index is None:
            new_tasks.append(Task(experiment=aux_experiment))
        else:
            new_tasks[index] = Task(experiment=aux_experiment)
        return new_tasks

    def _get_required_auxiliary_scans(
        self, experiment: Experiment
    ) -> list[AuxiliaryScan]:
        # This should be fleshed out https://github.com/DiamondLightSource/daq-queuing-service/issues/79
        time_per_pdf = experiment.experiment_definition.data["time_per_pdf"]
        assert experiment.sample
        return [
            AuxiliaryScan(
                instrument_session=experiment.instrument_session,
                pin=None,
                time_per_pdf=time_per_pdf,
            ),
            AuxiliaryScan(
                instrument_session=experiment.instrument_session,
                pin=StandardsPin(
                    capillary=experiment.sample.data["capillary"], contents=None
                ),
                time_per_pdf=time_per_pdf,
            ),
            AuxiliaryScan(
                instrument_session=experiment.instrument_session,
                pin=StandardsPin(
                    capillary=experiment.sample.data["capillary"], contents="Silicon"
                ),
                time_per_pdf=time_per_pdf,
            ),
        ]

    def _construct_auxiliary_experiment(
        self, auxiliary_scan: AuxiliaryScan, instrument_session: str
    ) -> Experiment:
        LOGGER.debug(f"Constructing experiment for auxiliary scan: {auxiliary_scan}")

        return Experiment(
            name=auxiliary_scan.kind,
            instrument_session=instrument_session,
            # Need to get sample info for test samples (air, empty capillary etc)
            sample=self._construct_auxiliary_sample(auxiliary_scan.pin),
            experiment_definition=ExperimentDefinition(
                name=f"Auxiliary {auxiliary_scan.kind} Scan",
                id="",
                data={"time_per_pdf": auxiliary_scan.time_per_pdf},
            ),
        )

    def _construct_auxiliary_sample(self, pin: StandardsPin | None):
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
