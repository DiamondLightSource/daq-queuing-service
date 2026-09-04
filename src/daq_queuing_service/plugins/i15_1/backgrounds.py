from typing import Literal

from pydantic import BaseModel, ConfigDict

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
    bg_type: BACKGROUND_TYPES
    time_per_pdf: float

    def add_tiled_id(self, tiled_id: str) -> "TiledBackground":
        return TiledBackground(
            bg_type=self.bg_type, tiled_id=tiled_id, time_per_pdf=self.time_per_pdf
        )

    def is_suitable(self, required_background: "BackgroundInfo") -> bool:
        """Determine if this background is suitable compared to an experiment's required
        background.

        Args:
            required_background (BackgroundInfo): The required background

        Returns:
            bool: True if suitable, False if not
        """
        return (
            self.bg_type == required_background.bg_type
            and self.time_per_pdf >= required_background.time_per_pdf
        )

    def get_matched_requirements(
        self, required_background: "BackgroundInfo"
    ) -> "BackgroundInfo | None":
        if not self.bg_type == required_background.bg_type:
            return

        return BackgroundInfo(
            bg_type=self.bg_type,
            time_per_pdf=max(self.time_per_pdf, required_background.time_per_pdf),
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str
