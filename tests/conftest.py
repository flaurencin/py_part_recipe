import pytest

import device_guard
from py_part_recipe.wipe import wipe_disk


def pytest_sessionstart(session):
    problems = list(device_guard.verify_environment())
    if problems:
        pytest.exit(
            "The tests destroy the devices they use and refuse to run:\n  - "
            + "\n  - ".join(problems),
            returncode=4,
        )
    device_guard.install()


@pytest.fixture
def clean_disks():
    """Start and finish with disks having nothing on them."""
    disks = ["/dev/loop100", "/dev/loop101"]
    for disk in disks:
        wipe_disk(disk)
    yield disks
    for disk in disks:
        wipe_disk(disk)
