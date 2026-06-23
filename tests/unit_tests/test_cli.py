import subprocess
import sys
from unittest.mock import patch

import pytest

from daq_queuing_service import __version__
from daq_queuing_service.__main__ import main


@pytest.mark.timeout(2)
def test_cli_version():
    cmd = [sys.executable, "-m", "daq_queuing_service", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__


def test_main():
    with patch("daq_queuing_service.__main__.uvicorn.run") as mock_run:
        main(args=[])

    mock_run.assert_called_once()

    _, kwargs = mock_run.call_args
    assert kwargs["host"] == "0.0.0.0"
    assert kwargs["port"] == 8000
