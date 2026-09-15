from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, computed_field

from daq_queuing_service.plugins.i15_1.standards import StandardsPin
from daq_queuing_service.task_queue.task import Experiment

AUXILIARY_SCAN = Literal["air", "empty capillary", "standard sample"]


class AuxiliaryScan(StrEnum):
    AIR = "Air"
    EMPTY_CAPILLARY = "Empty Capillary"
    STANDARD_SAMPLE = "Standard Sample"


AUXILIARY_SCAN_NAMES = [member.value for member in AuxiliaryScan]


def is_auxiliary_str(value: str) -> bool:
    return value in AUXILIARY_SCAN_NAMES


class BackgroundInfo(BaseModel):
    # Currently only room temperatures scans are supported
    # https://github.com/DiamondLightSource/daq-queuing-service/issues/84
    model_config = ConfigDict(frozen=True)
    instrument_session: str
    pin: StandardsPin | None
    time_per_pdf: float

    @computed_field
    @property
    def kind(self) -> AuxiliaryScan:
        if self.pin is None:
            return AuxiliaryScan.AIR
        if self.pin.contents is None:
            return AuxiliaryScan.EMPTY_CAPILLARY
        return AuxiliaryScan.STANDARD_SAMPLE

    def is_suitable(self, required_background: "BackgroundInfo") -> bool:
        """Determine if this background is suitable compared to an experiment's required
        background.

        Args:
            required_background (BackgroundInfo): The required background

        Returns:
            bool: True if suitable, False if not
        """
        return (
            self.instrument_session == required_background.instrument_session
            and self.pin == required_background.pin
            and self.time_per_pdf >= required_background.time_per_pdf
        )

    def attempt_to_combine_with(
        self, required_background: "BackgroundInfo"
    ) -> "BackgroundInfo | None":
        """Creates a background that combines the requirements of this background object
        and a provided required background, if possible.

        Args:
            required_background (BackgroundInfo): The required background

        Returns:
            BackgroundInfo | None: The combined background, or None if one is not
            possible.
        """
        if not self.instrument_session == required_background.instrument_session:
            return
        if not self.pin == required_background.pin:
            return

        return BackgroundInfo(
            instrument_session=self.instrument_session,
            pin=self.pin,
            time_per_pdf=max(self.time_per_pdf, required_background.time_per_pdf),
        )

    @classmethod
    def from_experiment(cls, experiment: Experiment) -> "BackgroundInfo":
        assert is_auxiliary_str(experiment.name), (
            f"This experiment is not a background scan: {experiment}"
        )

        if experiment.sample is None:
            pin = None
        else:
            pin = StandardsPin(
                capillary=experiment.sample.data["capillary"],
                contents=experiment.sample.data["composition"],
            )

        return cls(
            instrument_session=experiment.instrument_session,
            pin=pin,
            time_per_pdf=experiment.experiment_definition.data["time_per_pdf"],
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str
    filename: str
    filepath: Path
    instrument_session_directory: Path
