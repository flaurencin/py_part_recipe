import pytest

import device_guard


def pytest_sessionstart(session):
    problems = list(device_guard.verify_environment())
    if problems:
        pytest.exit(
            "The tests destroy the devices they use and refuse to run:\n  - "
            + "\n  - ".join(problems),
            returncode=4,
        )
    device_guard.install()
