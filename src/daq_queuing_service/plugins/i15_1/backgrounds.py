from typing import Literal

from pydantic import BaseModel, ConfigDict

# This should be generated from the json schema
# https://github.com/DiamondLightSource/daq-queuing-service/issues/78
BACKGROUND_TYPES = Literal["air", "bs", "fq", "pi"]


class BackgroundInfo(BaseModel):
    # Currently only room temperatures scans are supported
    # https://github.com/DiamondLightSource/daq-queuing-service/issues/84
    model_config = ConfigDict(frozen=True)
    bg_type: BACKGROUND_TYPES

    def add_tiled_id(self, tiled_id: str) -> "TiledBackground":
        return TiledBackground(
            bg_type=self.bg_type,
            tiled_id=tiled_id,
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str
