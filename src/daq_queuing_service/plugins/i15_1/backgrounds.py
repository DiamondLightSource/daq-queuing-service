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

TEMPERATURE_STEP = 100


def get_background_temperatures(
    list_of_temperatures: list[int], temperature_step: int = 100
):
    return list(
        range(
            min(list_of_temperatures),
            max(list_of_temperatures) + temperature_step,
            temperature_step,
        )
    )


class BackgroundInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    bg_type: CAPILLARIES | Literal["air"]
    time_per_pdf: int
    # Empty list or None signifies a room temperature collection
    list_of_temperatures: list[int] | None = None

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
        if bool(self.list_of_temperatures) and bool(
            required_background.list_of_temperatures
        ):
            if max(required_background.list_of_temperatures) > max(
                self.list_of_temperatures
            ) or min(required_background.list_of_temperatures) < min(
                self.list_of_temperatures
            ):
                return False
            if not all(
                # All required temperatures should be within 50C
                any(abs(temp1 - temp2) <= 50 for temp1 in self.list_of_temperatures)
                for temp2 in required_background.list_of_temperatures
            ):
                return False
        return (
            self.bg_type == required_background.bg_type
            and self.time_per_pdf >= required_background.time_per_pdf
        )

    def get_matched_requirements(
        self, required_background: "BackgroundInfo"
    ) -> "BackgroundInfo | None":
        if not self.bg_type == required_background.bg_type:
            return

        if not bool(self.list_of_temperatures) == bool(
            required_background.list_of_temperatures
        ):
            # Backgrounds cannot be matched if one is room temp and one is not
            return

        if required_background.list_of_temperatures and self.list_of_temperatures:
            list_of_temperatures = get_background_temperatures(
                required_background.list_of_temperatures + self.list_of_temperatures
            )
        else:
            list_of_temperatures = None

        background = BackgroundInfo(
            bg_type=self.bg_type,
            time_per_pdf=max(self.time_per_pdf, required_background.time_per_pdf),
            list_of_temperatures=list_of_temperatures,
        )
        assert background.is_suitable(required_background)
        return background


class TiledBackground(BackgroundInfo):
    tiled_id: str
