"""Helpers shared by the tests working on the test loop devices."""

import re
import subprocess

from py_part_recipe.partition_common import HandledPartitions


def make_parts(handled_parts: HandledPartitions):
    """Write the partitions and tell the kernel about them."""
    handled_parts.create_partitions_mapping()
    handled_parts.commit_to_devices()
    handled_parts.notify_kernel()


def signatures(device: str) -> dict[str, str]:
    """What blkid finds written on a device: a file system or a member of
    raid or lvm (TYPE), a partition table (PTTYPE).

    The details of the partition itself (PART_ENTRY_*, blkid gives them for any
    partition) are not a signature written on the device and are ignored.
    """
    result = subprocess.run(
        ["blkid", "-p", "-o", "export", device], capture_output=True
    )
    if result.returncode not in (0, 2):
        raise RuntimeError(f"blkid failed on {device}: {result.stderr!r}")
    found = {}
    for line in result.stdout.decode("utf-8").splitlines():
        key, _, value = line.partition("=")
        if key in ("TYPE", "PTTYPE"):
            found[key] = value
    return found


def signature_present(device: str) -> bool:
    return bool(signatures(device))


def write_at(device: str, offset: int, data: bytes):
    with open(device, "r+b") as dev:
        dev.seek(offset)
        dev.write(data)


def ext_uuid(device: str) -> str:
    out = subprocess.run(["dumpe2fs", "-h", device], capture_output=True, check=True)
    match = re.search(r"Filesystem UUID:\s*(\S+)", out.stdout.decode("utf-8"))
    assert match, f"no uuid in dumpe2fs of {device}"
    return match.group(1)
