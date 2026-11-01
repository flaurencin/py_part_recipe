import copy

import pytest

import device_guard
from device_guard import ensure_vg_absent
from samples import DISKS, RECIPE
from py_part_recipe.applier import apply_recipe
from py_part_recipe.mounting import UmountError, umount_all
from py_part_recipe.recipe_loader import parse_recipe
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


@pytest.fixture
def applied(tmp_path):
    """The recipe of the tests applied: the report, and the directory to mount in."""
    ensure_vg_absent("vg_test")
    for disk in DISKS:
        wipe_disk(disk)
    report = apply_recipe(parse_recipe(copy.deepcopy(RECIPE)))
    root = str(tmp_path / "target")
    yield report, root
    try:
        umount_all(report, root)
    except UmountError:
        pass  # wiping the disks then says what is busy
    for disk in DISKS:
        wipe_disk(disk)
