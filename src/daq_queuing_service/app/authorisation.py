from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException
from starlette.status import HTTP_403_FORBIDDEN

from daq_queuing_service.app.authentication import User


def build_ensure_current_user_is_in_whitelist(
    whitelist: list[str] | None, get_current_user: Callable[..., User]
) -> Callable[[User], User]:
    def ensure_current_user_is_in_whitelist(
        current_user: Annotated[User, Depends(get_current_user)],
    ) -> User:
        if whitelist is None or current_user.fedid in whitelist:
            return current_user
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Not authorised")

    return ensure_current_user_is_in_whitelist
