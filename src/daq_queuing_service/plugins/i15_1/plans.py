from enum import StrEnum
from typing import Any

from blueapi.service.model import TaskRequest

from daq_queuing_service.plugins.i15_1.auxiliary import (
    AuxiliaryScanType,
    TiledAuxiliary,
)
from daq_queuing_service.task_queue.task import Experiment, Sample


class ScanType(StrEnum):
    DATA_COLLECTION = "Data Collection"
    CENTRING = "Centring"
    AIR = AuxiliaryScanType.AIR
    EMPTY_CAPILLARY = AuxiliaryScanType.EMPTY_CAPILLARY
    STANDARD_SAMPLE = AuxiliaryScanType.STANDARD_SAMPLE


def get_robot_load(sample: Sample, instrument_session: str) -> TaskRequest:
    position = sample.positionInContainer.position
    puck = sample.container.positionInParent.position

    return TaskRequest(
        name="robot_load",
        params={"puck": puck, "position": position},
        instrument_session=instrument_session,
    )


def get_robot_unload(instrument_session: str) -> TaskRequest:
    return TaskRequest(
        name="robot_unload", params={}, instrument_session=instrument_session
    )


def get_wait_for_beam(instrument_session: str) -> TaskRequest:
    return TaskRequest(
        name="wait_for_beam",
        params={},
        instrument_session=instrument_session,
    )


def get_centre_sample(experiment: Experiment) -> TaskRequest:
    return TaskRequest(
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
    )


def get_data_collection(
    experiment: Experiment,
    tiled_auxiliary_scans: dict[AuxiliaryScanType, TiledAuxiliary] | None,
):

    experiment_definition = experiment.experiment_definition

    collection_metadata: dict[str, Any] = {
        "sample": experiment.sample,
        "experiment_definition": experiment.experiment_definition,
    }
    if tiled_auxiliary_scans:
        collection_metadata["auxiliary_scans"] = tiled_auxiliary_scans

    try:
        scan_type = ScanType(experiment.name)
    except ValueError:
        scan_type = ScanType.DATA_COLLECTION

    common_params: dict[str, Any] = {
        "exposure_time_per_frame": 0.1,
        "scan_type": scan_type,
        "metadata": collection_metadata,
    }

    # Assume collections with lists of temperatures are blowers, see
    # https://github.com/DiamondLightSource/crystallography-bluesky/issues/125
    if "list_of_temperatures" in experiment_definition.data.keys():
        data_collection = TaskRequest(
            name="blower_collection",
            params=common_params
            | {
                "time_per_collection": experiment_definition.data["time_per_pdf"],
                "ramp_rate_c_per_min": experiment_definition.data["ramp_rate"],
                "settle_time": experiment_definition.data["settle_time"],
                "temperatures_celsius": experiment_definition.data[
                    "list_of_temperatures"
                ],
            },
            instrument_session=experiment.instrument_session,
        )
    else:
        data_collection = TaskRequest(
            name="data_collection",
            params=common_params
            | {"full_collection_time": experiment_definition.data["time_per_pdf"]},
            instrument_session=experiment.instrument_session,
        )
    return data_collection
