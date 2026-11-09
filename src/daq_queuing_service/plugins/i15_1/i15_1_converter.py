from dataclasses import dataclass, replace
from functools import cached_property
from typing import Any

from blueapi.service.model import TaskRequest
from blueapi.worker.event import TaskError
from daq_config_server.client import ConfigClient
from daq_config_server.models.i15_1 import QueueParameters, StandardsPin, StandardsPuck
from tiled.client.container import Container as TiledContainer

from daq_queuing_service.external_interaction.blueapi.blueapi_call import (
    BlueapiCall,
    BlueapiCallResponse,
)
from daq_queuing_service.external_interaction.tiled.tiled import get_tiled_client
from daq_queuing_service.log import (
    LOGGER,
    add_session_to_logger,
)
from daq_queuing_service.plugins.converter import Converter
from daq_queuing_service.plugins.i15_1.auxiliary import (
    AuxiliaryScan,
    AuxiliaryScanType,
    TiledAuxiliary,
    get_background_temperatures,
    is_auxiliary_experiment,
)
from daq_queuing_service.plugins.i15_1.plans import (
    experiment_is_blower,
    get_centre_sample,
    get_data_collection,
    get_robot_load,
    get_robot_unload,
    get_wait_for_beam,
)
from daq_queuing_service.plugins.i15_1.tiled_interaction import get_suitable_tiled_scan
from daq_queuing_service.task_queue.task import (
    Container,
    ContainerPosition,
    Experiment,
    ExperimentDefinition,
    Sample,
    Task,
    TaskWithPosition,
)

CONFIG_SERVER_URL = "https://i15-1-daq-config.diamond.ac.uk"
STANDARDS_PUCK_CONFIG_PATH = (
    "/dls_sw/i15-1/software/daq_configuration/standards_puck.json"
)
QUEUE_PARAMS_CONFIG_PATH = (
    "/dls_sw/i15-1/software/daq_configuration/queue_parameters.toml"
)


@dataclass(frozen=True)
class QueuedAuxiliary:
    index: int
    task_id: str
    auxiliary: AuxiliaryScan


def _filter_auxiliary_scans(tasks: list[Task]) -> list[QueuedAuxiliary]:
    return [
        QueuedAuxiliary(i, task.id, AuxiliaryScan.from_experiment(task.experiment))
        for i, task in enumerate(tasks)
        if isinstance(task.experiment, Experiment)
        and is_auxiliary_experiment(task.experiment)
    ]


class I151Converter(Converter):
    def __init__(self):
        # First key is the ID of the task using the auxiliary scan
        self._tiled_auxiliary_scans: dict[
            str, dict[AuxiliaryScanType, TiledAuxiliary]
        ] = {}
        self._config_client = ConfigClient.from_url(CONFIG_SERVER_URL)

    @cached_property
    def _tiled_client(self) -> TiledContainer:
        return get_tiled_client()

    @property
    def _standards_puck(self) -> StandardsPuck:
        return self._config_client.get_file_contents(
            STANDARDS_PUCK_CONFIG_PATH, desired_return_type=StandardsPuck
        )

    @property
    def _queue_params(self) -> QueueParameters:
        return self._config_client.get_file_contents(
            QUEUE_PARAMS_CONFIG_PATH, desired_return_type=QueueParameters
        )

    def pre_process(
        self,
        current_task: TaskWithPosition | None,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCallResponse],
    ) -> list[Task]:
        self._config_client.reset_cache()
        if self._queue_params.add_auxiliary_scans:
            return self._add_required_auxiliary_scans(current_task, queue)
        return queue

    def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCallResponse],
    ) -> list[BlueapiCall]:
        call_list: list[BlueapiCall] = []

        for task in queue:
            with add_session_to_logger(task.experiment.instrument_session):
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
                                BlueapiCall(
                                    task_request=b_api_task, parent_task_id=task.id
                                )
                                for b_api_task in (
                                    self._construct_blueapi_tasks_from_experiment(
                                        task.experiment, task.id
                                    )
                                )
                            ]
                        )
        return call_list

    def pause_on_fail_blueapi_call(
        self, call: BlueapiCall, errors: list[str | TaskError] | None
    ) -> bool:

        for error in errors or []:
            if isinstance(error, TaskError):
                if "Robot load failed, no sample found at puck" in error.message:
                    LOGGER.warning(
                        "Robot load failed, no sample found. "
                        + "Continuing with the rest of the queue"
                    )
                    return False
        return True

    def _construct_blueapi_tasks_from_experiment(
        self, experiment: Experiment, task_id: str
    ) -> list[TaskRequest]:
        LOGGER.debug(f"Converting to blueapi calls, experiment = {experiment}")

        session = experiment.instrument_session
        sample = experiment.sample
        if experiment_is_blower(experiment):
            centring_params = self._queue_params.blower_centring_params
        else:
            centring_params = self._queue_params.room_temperature_centring_params
        data_collection_params = self._queue_params.data_collection_params
        tiled_scans = self._tiled_auxiliary_scans.get(task_id)

        plans: list[TaskRequest] = []

        if sample and self._queue_params.do_robot_load:
            plans.append(get_robot_load(sample, session))

        if self._queue_params.do_wait_for_beam:
            plans.append(get_wait_for_beam(session))

        if sample and self._queue_params.do_centring:
            plans.append(get_centre_sample(centring_params, experiment))
            if self._queue_params.do_wait_for_beam:
                plans.append(get_wait_for_beam(session))

        plans.append(
            get_data_collection(data_collection_params, experiment, tiled_scans)
        )

        if sample and self._queue_params.do_robot_load:
            plans.append(get_robot_unload(session))

        return plans

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
            with add_session_to_logger(task.experiment.instrument_session):
                experiment = task.experiment
                if isinstance(experiment, Experiment):
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
    ) -> list[Task]:
        if (
            current_task
            and isinstance(current_task.experiment, Experiment)
            and is_auxiliary_experiment(current_task.experiment)
            and AuxiliaryScan.from_experiment(current_task.experiment).is_suitable(
                required_auxiliary
            )
        ):
            return new_tasks

        queued_auxiliary_scans = _filter_auxiliary_scans(new_tasks)
        if any(
            queued_auxiliary.auxiliary.is_suitable(required_auxiliary)
            for queued_auxiliary in queued_auxiliary_scans
        ):
            return new_tasks

        if tiled_scan := get_suitable_tiled_scan(
            self._tiled_client,
            required_auxiliary,
            self._queue_params.tiled_stale_time_s,
        ):
            self._tiled_auxiliary_scans[task_id][tiled_scan.kind] = tiled_scan
            return new_tasks

        LOGGER.info(
            f"No existing suitable auxiliary scans found for {required_auxiliary}, "
            + "modifying or adding one",
        )
        return self._add_or_replace_auxiliary_scan(
            required_auxiliary,
            current_task,
            new_tasks,
            queued_auxiliary_scans,
            instrument_session,
        )

    def _add_or_replace_auxiliary_scan(
        self,
        required_auxiliary: AuxiliaryScan,
        current_task: TaskWithPosition | None,
        new_tasks: list[Task],
        queued_auxiliary_scans: list[QueuedAuxiliary],
        instrument_session: str,
    ) -> list[Task]:
        # It is assumed every scan in queued_auxiliary_scans has already added their
        # requirements before for them
        combined_auxiliary = self._find_combinable_auxiliary(
            required_auxiliary, queued_auxiliary_scans
        )

        aux_experiment = self._construct_auxiliary_experiment(
            combined_auxiliary.auxiliary if combined_auxiliary else required_auxiliary,
            instrument_session,
        )

        if combined_auxiliary is None:
            aux_task = Task(experiment=aux_experiment)
            prefix = new_tasks
            suffix = []
            additional_required = required_auxiliary.requires
        else:
            aux_task = Task(experiment=aux_experiment, id=combined_auxiliary.task_id)
            prefix = new_tasks[: combined_auxiliary.index]
            suffix = new_tasks[combined_auxiliary.index + 1 :]
            additional_required = combined_auxiliary.auxiliary.requires

        for required in additional_required:
            prefix = self._ensure_auxiliary_in_queue_or_tiled(
                required,
                current_task,
                prefix,
                aux_task.id,
                instrument_session,
            )
        new_tasks = prefix + [aux_task] + suffix

        return new_tasks

    def _find_combinable_auxiliary(
        self,
        required_auxiliary: AuxiliaryScan,
        queued_auxiliary_scans: list[QueuedAuxiliary],
    ):
        for queued_auxiliary in queued_auxiliary_scans:
            if (
                combined_auxiliary_scan
                := queued_auxiliary.auxiliary.attempt_to_combine_with(
                    required_auxiliary
                )
            ):
                return replace(queued_auxiliary, auxiliary=combined_auxiliary_scan)

    def _get_required_auxiliary_scans(
        self, experiment: Experiment
    ) -> list[AuxiliaryScan]:
        if is_auxiliary_experiment(experiment):
            return experiment.experiment_definition.data["required_auxiliary_scans"]

        time_per_pdf = experiment.experiment_definition.data["time_per_pdf"]
        list_of_temperatures = experiment.experiment_definition.data.get(
            "list_of_temperatures"
        )
        if list_of_temperatures:
            list_of_temperatures = get_background_temperatures(list_of_temperatures)

        assert experiment.sample

        air = AuxiliaryScan(
            instrument_session=experiment.instrument_session,
            pin=None,
            time_per_pdf=time_per_pdf,
        )
        empty_capillary = AuxiliaryScan(
            instrument_session=experiment.instrument_session,
            pin=StandardsPin(
                capillary=experiment.sample.data["capillary"], contents=None
            ),
            time_per_pdf=time_per_pdf,
            list_of_temperatures=list_of_temperatures,
            requires=[air],
        )
        standard_sample = AuxiliaryScan(
            instrument_session=experiment.instrument_session,
            pin=StandardsPin(
                capillary=experiment.sample.data["capillary"], contents="Silicon"
            ),
            time_per_pdf=time_per_pdf,
            requires=[air, empty_capillary],
        )

        return [standard_sample, empty_capillary, air]

    def _construct_auxiliary_experiment(
        self, auxiliary_scan: AuxiliaryScan, instrument_session: str
    ) -> Experiment:
        LOGGER.debug(f"Constructing experiment for auxiliary scan: {auxiliary_scan}")
        data: dict[str, Any] = {
            "time_per_pdf": auxiliary_scan.time_per_pdf,
            "required_auxiliary_scans": auxiliary_scan.requires,
        }
        if auxiliary_scan.list_of_temperatures:
            settle_time = (
                self._queue_params.blower_auxiliary_collection_params.settle_time_s
            )
            ramp_rate = self._queue_params.blower_auxiliary_collection_params.ramp_rate
            data.update(
                {
                    "list_of_temperatures": auxiliary_scan.list_of_temperatures,
                    "ramp_rate": ramp_rate,
                    "settle_time": settle_time,
                }
            )

        sample = self._construct_auxiliary_sample(auxiliary_scan.pin)
        experiment_name = (
            f"{sample.name} - {auxiliary_scan.kind}"
            if sample
            else f"{auxiliary_scan.kind}"
        )

        return Experiment(
            name=experiment_name,
            instrument_session=instrument_session,
            sample=sample,
            experiment_definition=ExperimentDefinition(
                name=auxiliary_scan.kind,
                id="",
                data=data,
            ),
        )

    def _construct_auxiliary_sample(self, pin: StandardsPin | None):
        if pin is None:
            return

        data = {"capillary": pin.capillary, "composition": pin.contents}
        puck = Container(
            id="",
            positionInParent=ContainerPosition(
                position=self._standards_puck.position_on_table
            ),
        )
        position = ContainerPosition(
            position=self._standards_puck.get_position_of_pin(pin)
        )

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
