from typing import Literal

from pydantic import BaseModel, ConfigDict

BACKGROUND = Literal["air", "capillary_1", "capillary_2"]


class BackgroundInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    bg_type: BACKGROUND
    cobra: bool
    blower: bool

    def add_tiled_id(self, tiled_id: str) -> "TiledBackground":
        return TiledBackground(
            bg_type=self.bg_type,
            cobra=self.cobra,
            blower=self.blower,
            tiled_id=tiled_id,
        )


class TiledBackground(BackgroundInfo):
    tiled_id: str
