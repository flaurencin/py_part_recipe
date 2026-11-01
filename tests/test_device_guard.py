import subprocess

import parted
import pytest

import device_guard
from device_guard import DeviceGuardError

FORBIDDEN = (
    "/dev/sda",
    "/dev/nvme0n1p1",
    "/dev/loop0",
    "/dev/loop107",
    "/dev/loop119",
    "/dev/loop122",
    "/dev/md0",
)
ALLOWED = (
    "/dev/loop100",
    "/dev/loop106p1",
    "/dev/md100",
    "/dev/loop120",
    "/dev/loop121p2",
    "/dev/md199",
    "/dev/vg_test",
    "/dev/vg_test/lv_test",
    "/dev/null",
)


@pytest.mark.parametrize("path", FORBIDDEN)
def test_path_refused(path):
    with pytest.raises(DeviceGuardError, match=path):
        device_guard.check_device_path(path, "test")


@pytest.mark.parametrize("path", ALLOWED + ("/tmp/loop0", "relative", None))
def test_path_allowed(path):
    device_guard.check_device_path(path, "test")


@pytest.mark.parametrize("path", FORBIDDEN)
def test_path_traversal_is_refused(path):
    with pytest.raises(DeviceGuardError):
        device_guard.check_device_path(f"/dev/loop100/../{path[5:]}", "test")


@pytest.mark.parametrize("path", FORBIDDEN)
def test_subprocess_refused(path):
    with pytest.raises(DeviceGuardError):
        subprocess.run(["true", path])
    with pytest.raises(DeviceGuardError):
        subprocess.run(["true", f"--device={path}"])
    with pytest.raises(DeviceGuardError):
        subprocess.run(f"true {path}", shell=True)
    with pytest.raises(DeviceGuardError):
        subprocess.check_output(["true", path])


@pytest.mark.parametrize("path", ALLOWED)
def test_subprocess_allowed(path):
    assert subprocess.run(["true", path]).returncode == 0


@pytest.mark.parametrize("path", FORBIDDEN)
def test_parted_refused(path):
    with pytest.raises(DeviceGuardError):
        parted.Device(path)
    with pytest.raises(DeviceGuardError):
        parted.getDevice(path)


@pytest.mark.parametrize("path", FORBIDDEN)
@pytest.mark.parametrize("mode", ("r+b", "wb", "ab", "w"))
def test_open_for_writing_refused(path, mode):
    with pytest.raises(DeviceGuardError):
        open(path, mode)


def test_open_for_reading_is_not_checked():
    with open("/dev/null", "rb") as dev:
        assert dev.read() == b""


@pytest.mark.parametrize(
    "name", ("root", "vg00-root", "dm-0", "vg_test", "vg_test-lv_test", "vg_testing-lv")
)
def test_dmsetup_remove(name):
    allowed = name.startswith("vg_test")
    command = ["true", "dmsetup", "remove", "--force", name]
    if allowed:
        assert subprocess.run(command).returncode == 0
    else:
        with pytest.raises(DeviceGuardError, match=name):
            subprocess.run(command)


@pytest.mark.parametrize("sub", ("remove_all", "wipe_table", "suspend", "create"))
def test_dmsetup_other_commands_refused(sub):
    with pytest.raises(DeviceGuardError, match=sub):
        subprocess.run(["true", "dmsetup", sub, "vg_test"])


@pytest.mark.parametrize("sub", ("ls", "info", "status"))
def test_dmsetup_read_only_commands_allowed(sub):
    assert subprocess.run(["true", "dmsetup", sub]).returncode == 0


def test_environment_has_no_problem():
    assert list(device_guard.verify_environment()) == []
