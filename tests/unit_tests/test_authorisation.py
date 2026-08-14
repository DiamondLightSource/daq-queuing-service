from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from daq_queuing_service.app.authentication import User
from daq_queuing_service.app.authorisation import (
    build_ensure_current_user_is_in_whitelist,
)


def test_ensure_current_user_is_in_whitelist_returns_user_if_user_in_whitelist():
    user = User(fedid="abc12345")

    whitelist_check = build_ensure_current_user_is_in_whitelist(
        ["abc12345", "def67890"], MagicMock()
    )
    assert whitelist_check(user) == user


def test_ensure_current_user_is_in_whitelist_raises_error_if_user_not_in_whitelist():
    user = User(fedid="abc12345")

    whitelist_check = build_ensure_current_user_is_in_whitelist(
        ["def67890"], MagicMock()
    )
    with pytest.raises(HTTPException):
        whitelist_check(user)


def test_ensure_current_user_is_in_whitelist_returns_user_if_no_whitelist_provided():
    user = User(fedid="abc12345")

    whitelist_check = build_ensure_current_user_is_in_whitelist(None, MagicMock())
    assert whitelist_check(user) == user
