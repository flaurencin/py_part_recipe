"""Make sure the tests never touch anything but the test loop devices.

The tests partition, format and build RAID/LVM volumes for real, so a wrong
device path would destroy real data. Two layers protect against that:

1. At the start of the session, refuse to run if the names the tests use
   are already taken by something that is not a test device: a loop device
   not backed by /tmp/loopN, a md array named md1NN, a volume group vg_test*.
2. Every program started by the tests (subprocess), every parted device and
   every write open of a /dev file is checked: a /dev path that is not a
   test device raises RuntimeError before anything happens.

The real functions are still called, only the arguments are checked.
"""

import builtins
import glob
import os
import re
import shlex
import subprocess
from typing import Any, Iterable

import parted

# 100 to 106 are the devices of the tests, big enough to apply the example
# recipe; 120 and 121 stay small, the tests of the sizes need exact geometries
TEST_LOOP_NUMBERS = (*range(100, 107), 120, 121)
TEST_LOOPS = [f"loop{number}" for number in TEST_LOOP_NUMBERS]
BACKING_DIRECTORY = "/tmp"

# Paths the tests are allowed to give to a command or to open for writing:
# the test loop devices and their partitions, the md arrays md100 to md199,
# and the volume groups (and logical volumes) named vg_test*.
ALLOWED_DEVICE_RE = re.compile(
    r"^/dev/("
    r"loop(10[0-6]|12[01])(p\d+)?"
    r"|md1\d\d"
    r"|vg_test[\w-]*(/[\w-]+)?"
    r"|mapper/vg_test[\w-]*"
    r")$"
)
HARMLESS_DEVICES = ("/dev/null", "/dev/zero")
# dmsetup works on device mapper names (vg-lv), not on /dev paths: only the
# names of the test volume groups can be removed, the rest is read only
ALLOWED_DM_NAME_RE = re.compile(r"^vg_test[\w]*(-[\w]+)?$")
DMSETUP_READ_ONLY = ("ls", "info", "status", "table", "deps", "version")


class DeviceGuardError(RuntimeError):
    pass


def check_device_path(path: Any, origin: str):
    """Raise unless `path` is not a device or is a test device."""
    path = os.fspath(path) if isinstance(path, os.PathLike) else path
    if isinstance(path, bytes):
        path = os.fsdecode(path)
    if not isinstance(path, str) or not path.startswith("/dev/"):
        return
    path = os.path.normpath(path)
    if path in HARMLESS_DEVICES or ALLOWED_DEVICE_RE.match(path):
        return
    raise DeviceGuardError(
        f"{origin}: refusing to use {path}, tests may only use the loop "
        "devices /dev/loop100-106 and 120-121, md arrays /dev/md100-199 and vg_test* "
        "volume groups"
    )


def _check_dmsetup(args: list, origin: str):
    """Check the arguments of a dmsetup command (`dmsetup <command> names`)."""
    position = next(
        (i for i, arg in enumerate(args) if os.path.basename(arg) == "dmsetup"), None
    )
    if position is None:
        return
    rest = [arg for arg in args[position + 1 :] if not arg.startswith("-")]
    if not rest or rest[0] in DMSETUP_READ_ONLY:
        return
    if rest[0] != "remove":
        raise DeviceGuardError(f"{origin}: dmsetup {rest[0]} is not allowed in tests")
    for name in rest[1:]:
        if not ALLOWED_DM_NAME_RE.match(name):
            raise DeviceGuardError(
                f"{origin}: refusing to remove the device mapper device {name}, "
                "tests may only remove vg_test* volume groups"
            )


def _check_command(args: Any, shell: bool, origin: str):
    if isinstance(args, (str, bytes, os.PathLike)):
        args = shlex.split(os.fsdecode(args)) if shell else [args]
    args = [
        os.fsdecode(arg) if isinstance(arg, (bytes, os.PathLike)) else str(arg)
        for arg in args
    ]
    _check_dmsetup(args, origin)
    for arg in args:
        # options like --device=/dev/sda are checked too
        check_device_path(arg, origin)
        if "=" in arg:
            check_device_path(arg.split("=", 1)[1], origin)


def verify_environment() -> Iterable[str]:
    """Names of problems that make running the tests dangerous."""
    problems = []
    for name in TEST_LOOPS:
        device = f"/dev/{name}"
        if not os.path.exists(device):
            continue  # missing devices are not dangerous, tests will fail
        sysfs = f"/sys/block/{name}/loop/backing_file"
        try:
            with open(sysfs, encoding="utf-8") as backing:
                backing_file = backing.read().strip()
        except OSError:
            problems.append(f"{device} exists but is not a configured loop device")
            continue
        expected = f"{BACKING_DIRECTORY}/{name}"
        if backing_file != expected:
            problems.append(f"{device} is backed by {backing_file}, not {expected}")
    for pattern in ("/dev/md1[0-9][0-9]", "/dev/vg_test*"):
        for found in sorted(glob.glob(pattern)):
            problems.append(f"{found} exists, tests would clean it up")
    try:
        with open("/proc/mdstat", encoding="utf-8") as mdstat:
            for line in mdstat:
                if re.match(r"^md1\d\d\s*:", line):
                    problems.append(f"md array {line.split()[0]} is active")
    except OSError:
        pass
    return problems


def install():
    """Check the arguments of everything that can touch a device."""
    original_popen_init = subprocess.Popen.__init__

    def guarded_popen_init(self, args, *pargs, **kwargs):
        _check_command(args, bool(kwargs.get("shell")), "subprocess")
        original_popen_init(self, args, *pargs, **kwargs)

    subprocess.Popen.__init__ = guarded_popen_init

    original_open = builtins.open

    def guarded_open(file, mode="r", *pargs, **kwargs):
        if isinstance(file, (str, bytes, os.PathLike)) and any(
            flag in str(mode) for flag in "wax+"
        ):
            check_device_path(file, "open")
        return original_open(file, mode, *pargs, **kwargs)

    builtins.open = guarded_open

    class GuardedDevice(parted.Device):
        def __init__(self, path=None, *pargs, **kwargs):
            check_device_path(path, "parted.Device")
            super().__init__(path, *pargs, **kwargs)

    parted.Device = GuardedDevice

    original_get_device = parted.getDevice

    def guarded_get_device(path):
        check_device_path(path, "parted.getDevice")
        return original_get_device(path)

    parted.getDevice = guarded_get_device

    for name in ("freshDisk", "newDisk"):
        original = getattr(parted, name)

        def guarded(device, *pargs, _original=original, _name=name, **kwargs):
            check_device_path(device.path, f"parted.{_name}")
            return _original(device, *pargs, **kwargs)

        setattr(parted, name, guarded)


def ensure_vg_absent(vg_name: str):
    """Fail unless no volume group has this name (needs the lvm tools).

    Tests remove their volume group by name when they finish, so a real
    volume group with the same name must never exist.
    """
    import pytest

    result = subprocess.run(
        ["sudo", "vgs", "--noheadings", "-o", "vg_name"], capture_output=True
    )
    if result.returncode != 0:
        pytest.fail(
            "Can not list the lvm volume groups, refusing to run a test that "
            "removes the volume group by name",
            pytrace=False,
        )
    names = result.stdout.decode("utf-8").split()
    if vg_name in names:
        pytest.fail(f"A volume group named {vg_name} already exists", pytrace=False)
