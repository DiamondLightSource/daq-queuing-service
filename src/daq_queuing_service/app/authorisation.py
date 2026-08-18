from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException
from starlette.status import HTTP_403_FORBIDDEN

from daq_queuing_service.app.authentication import User
from daq_queuing_service.worker.worker import LOGGER


def build_ensure_current_user_is_in_whitelist(
    whitelist: list[str] | None, get_current_user: Callable[..., User]
) -> Callable[[User], User]:
    def ensure_current_user_is_in_whitelist(
        current_user: Annotated[User, Depends(get_current_user)],
    ) -> User:
        LOGGER.debug(f"Got user: {current_user}")
        if whitelist is None:
            LOGGER.debug("No user whitelist. All authenticated users are authorised.")
            return current_user
        elif current_user.fedid in whitelist:
            LOGGER.debug(
                f"FedID {current_user.fedid} found in whitelist, user authorised."
            )
            return current_user
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Not authorised. You are not in the whitelist of authorised FedIDs",
        )

    return ensure_current_user_is_in_whitelist
