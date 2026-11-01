"""Wipe disks and partitions, as much as possible.

Old signatures (file systems, RAID or LVM members, partition tables) left on
a disk get in the way of a new layout: a RAID member superblock is found
again and the array is assembled, mdadm and pvcreate ask for confirmations,
blkid reports an old file system.

Wiping a disk is done in two steps:

1. release the disk: whatever is built on top of the disk or its partitions
   (RAID arrays, device mapper devices like LVM volumes or encrypted volumes,
   in any depth) is stopped, as a busy device can not be wiped
2. erase every signature (`wipefs`) of each partition, then of the disk,
   which removes the partition table, and tell the kernel the partitions are
   gone

Everything is destroyed without question, this is meant to be used only when
the caller asked for it. A file system that is mounted is not unmounted:
the device is busy and wiping fails with a WipeError.
"""

import os
import subprocess

from py_part_recipe.common import gen_cmd_for_subprocess

SYS_CLASS_BLOCK = "/sys/class/block"


class WipeError(Exception): ...


def _run(command: list[str]):
    result = subprocess.run(gen_cmd_for_subprocess(command), capture_output=True)
    if result.returncode != 0:
        error = result.stderr.decode("utf-8", errors="replace").strip()
        raise WipeError(
            f"Command: {' '.join(command)} Failed ({result.returncode}). {error}"
        )


def wipe_signatures(device: str):
    """Erase every signature of a device (`wipefs -a -f`)."""
    _run(["wipefs", "--all", "--force", device])


def _block_name(device: str) -> str:
    return os.path.basename(os.path.realpath(device))


def _sys_dir(name: str) -> str:
    return os.path.join(SYS_CLASS_BLOCK, name)


def partitions_of(disk_name: str) -> list[str]:
    """Names of the partitions of a disk known by the kernel."""
    disk_dir = _sys_dir(disk_name)
    return sorted(
        name
        for name in os.listdir(disk_dir)
        if os.path.exists(os.path.join(disk_dir, name, "partition"))
    )


def holders_of(name: str) -> list[str]:
    """Names of the devices built directly on top of a device."""
    holders_dir = os.path.join(_sys_dir(name), "holders")
    if not os.path.isdir(holders_dir):
        return []
    return sorted(os.listdir(holders_dir))


def _stop(holder: str):
    if holder.startswith("md"):
        _run(["mdadm", "--stop", f"/dev/{holder}"])
    elif holder.startswith("dm-"):
        with open(
            os.path.join(_sys_dir(holder), "dm", "name"), encoding="utf-8"
        ) as dm_name:
            _run(["dmsetup", "remove", dm_name.read().strip()])
    else:
        raise WipeError(f"Do not know how to stop {holder} which uses the device")


def release(name: str):
    """Stop everything built on top of a device, from the top down."""
    for holder in holders_of(name):
        release(holder)
        _stop(holder)


def forget_partitions(device: str):
    """Tell the kernel the disk has no partition anymore.

    Erasing the partition table makes the kernel drop the partitions on most
    disks, but not on devices that do not scan their partitions, like loop
    devices set up without it: they would stay listed until someone says so.
    """
    name = _block_name(device)
    numbers = []
    for partition in partitions_of(name):
        with open(
            os.path.join(_sys_dir(partition), "partition"), encoding="utf-8"
        ) as number:
            numbers.append(int(number.read()))
    if not numbers:
        return
    _run(["partx", "--delete", "--nr", f"{min(numbers)}:{max(numbers)}", device])
    if partitions_of(name):
        raise WipeError(f"The kernel still lists partitions of {device}")


def wipe_disk(device: str):
    """Release a disk, erase the signatures of its partitions and of the disk.

    Args:
        device (str): whole disk, like /dev/sdb

    Raises:
        ValueError: device is not a whole disk known by the kernel
        WipeError: a command failed, for example the device is in use
    """
    name = _block_name(device)
    if not os.path.isdir(_sys_dir(name)):
        raise ValueError(f"{device} is not a block device")
    if os.path.exists(os.path.join(_sys_dir(name), "partition")):
        raise ValueError(f"{device} is a partition, a whole disk is expected")
    partitions = partitions_of(name)
    for partition in partitions:
        release(partition)
    release(name)
    for partition in partitions:
        wipe_signatures(f"/dev/{partition}")
    wipe_signatures(device)
    forget_partitions(device)
