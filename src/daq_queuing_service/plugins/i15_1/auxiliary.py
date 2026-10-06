from enum import StrEnum
from pathlib import Path

from daq_config_server.models.i15_1.standards_puck import StandardsPin
from pydantic import BaseModel, ConfigDict, computed_field

from daq_queuing_service.task_queue.task import Experiment

DEFAULT_TEMPERATURE_STEP = 100


class AuxiliaryScanType(StrEnum):
    AIR = "Air"
    EMPTY_CAPILLARY = "Empty Capillary"
    STANDARD_SAMPLE = "Standard Sample"


def is_auxiliary_str(value: str) -> bool:
    return value in list(AuxiliaryScanType)


class AuxiliaryScan(BaseModel):
    """Auxiliary scans are scans added automatically by the queue, such as background
    scans and standard sample calibration scans. They are needed for analysis of user
    sample data collections.
    """

    # Currently only single temperature scans are supported
    # https://github.com/DiamondLightSource/daq-queuing-service/issues/84
    model_config = ConfigDict(frozen=True)
    instrument_session: str
    pin: StandardsPin | None
    time_per_pdf: float
    list_of_temperatures: list[int] | None = None

    @computed_field
    @property
    def kind(self) -> AuxiliaryScanType:
        if self.pin is None:
            return AuxiliaryScanType.AIR
        if self.pin.contents is None:
            return AuxiliaryScanType.EMPTY_CAPILLARY
        return AuxiliaryScanType.STANDARD_SAMPLE

    def is_suitable(
        self,
        required_background: "AuxiliaryScan",
        temperature_step: int = DEFAULT_TEMPERATURE_STEP,
    ) -> bool:
        """Determine if this background is suitable compared to an experiment's required
        background.

        Args:
            required_background (AuxiliaryScan): The required background

        Returns:
            bool: True if suitable, False if not
        """
        if required_background.list_of_temperatures:
            if not self.list_of_temperatures:
                return False

            if max(required_background.list_of_temperatures) > max(
                self.list_of_temperatures
            ) or min(required_background.list_of_temperatures) < min(
                self.list_of_temperatures
            ):
                return False

            if not all(
                # All required temperatures should be within 50C
                any(
                    abs(temp1 - temp2) <= temperature_step / 2
                    for temp1 in self.list_of_temperatures
                )
                for temp2 in required_background.list_of_temperatures
            ):
                return False
        else:
            if self.list_of_temperatures:
                return False

        return (
            self.instrument_session == required_background.instrument_session
            and self.pin == required_background.pin
            and self.time_per_pdf >= required_background.time_per_pdf
        )

    def attempt_to_combine_with(
        self,
        required_background: "AuxiliaryScan",
        temperature_step: int = DEFAULT_TEMPERATURE_STEP,
    ) -> "AuxiliaryScan | None":
        """Creates a background that combines the requirements of this background object
        and a provided required background, if possible.

        Args:
            required_background (AuxiliaryScan): The required background

        Returns:
            AuxiliaryScan | None: The combined background, or None if one is not
            possible.
        """
        if not self.instrument_session == required_background.instrument_session:
            return
        if not self.pin == required_background.pin:
            return

        if bool(self.list_of_temperatures) is not bool(
            required_background.list_of_temperatures
        ):
            # Backgrounds cannot be matched if one is room temp and one is not
            return

        if required_background.list_of_temperatures and self.list_of_temperatures:
            list_of_temperatures = self.get_background_temperatures(
                required_background.list_of_temperatures + self.list_of_temperatures,
                temperature_step=temperature_step,
            )
        else:
            list_of_temperatures = None

        result = AuxiliaryScan(
            instrument_session=self.instrument_session,
            pin=self.pin,
            time_per_pdf=max(self.time_per_pdf, required_background.time_per_pdf),
            list_of_temperatures=list_of_temperatures,
        )
        assert result.is_suitable(required_background)
        return result

    @classmethod
    def from_experiment(cls, experiment: Experiment) -> "AuxiliaryScan":
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
            list_of_temperatures=experiment.experiment_definition.data.get(
                "list_of_temperatures"
            ),
        )

    @staticmethod
    def get_background_temperatures(
        list_of_temperatures: list[int],
        temperature_step: int = DEFAULT_TEMPERATURE_STEP,
    ):
        return list(
            range(
                min(list_of_temperatures),
                max(list_of_temperatures) + temperature_step,
                temperature_step,
            )
        )


class TiledAuxiliary(AuxiliaryScan):
    tiled_id: str
    filename: str
    filepath: Path
    instrument_session_directory: Path
