from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from daq_queuing_service.task_queue.task import Experiment

BACKGROUND_SCAN = "Background"
# This should be generated from the json schema
# https://github.com/DiamondLightSource/daq-queuing-service/issues/78
CAPILLARIES = Literal[
    "bs1.0",
    "bs1.5",
    "bs2.0",
    "fq0.4",
    "fq1.0",
    "fq1.5",
    "fq2.0",
    "fq2.5",
    "fq3.17",
    "pi1.0",
    "pi1.5",
    "pi2.0",
]
BACKGROUND_TYPES = CAPILLARIES | Literal["air"]


class BackgroundInfo(BaseModel):
    # Currently only room temperatures scans are supported
    # https://github.com/DiamondLightSource/daq-queuing-service/issues/84
    model_config = ConfigDict(frozen=True)
    instrument_session: str
    bg_type: BACKGROUND_TYPES
    time_per_pdf: float

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
            and self.bg_type == required_background.bg_type
            and self.time_per_pdf >= required_background.time_per_pdf
        )

    def get_matched_requirements(
        self, required_background: "BackgroundInfo"
    ) -> "BackgroundInfo | None":
        if not self.instrument_session == required_background.instrument_session:
            return
        if not self.bg_type == required_background.bg_type:
            return

        return BackgroundInfo(
            instrument_session=self.instrument_session,
            bg_type=self.bg_type,
            time_per_pdf=max(self.time_per_pdf, required_background.time_per_pdf),
        )

    @classmethod
    def from_experiment(cls, experiment: Experiment) -> "BackgroundInfo":
        assert experiment.name == BACKGROUND_SCAN, (
            f"This experiment is not a background scan: {experiment}"
        )
        return cls(
            instrument_session=experiment.instrument_session,
            bg_type=experiment.sample.data["capillary"],
            time_per_pdf=experiment.experiment_definition.data["time_per_pdf"],
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str
    filepath: Path
