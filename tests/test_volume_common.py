import logging
import os
import subprocess
from time import sleep
from typing import List
import pytest
from py_part_recipe.partition_common import (
    HandledPartitions,
    PartitionRequest,
    Partitionner,
    Recipe,
)
from parted import IOException, Partition

from py_part_recipe.volume_common import RaidVolume

LOGGER = logging.getLogger()


def setup_raid_disk(disks: List[str]) -> Partitionner:
    partitions = [
        PartitionRequest("efi_part", "2MiB", "5MiB", 100, flags=["raid", "esp"]),
        PartitionRequest("boot_part", "4MiB", "10MiB", 100, flags=["raid"]),
        PartitionRequest("root_part", "10MiB", "20MiB", 100, flags=["raid"]),
        PartitionRequest("var_part", "5MiB", "5MiB", 100, flags=["raid"]),
    ]
    main_recipe = Recipe(disks, partitions)
    main_parter = Partitionner(main_recipe)
    return main_parter


@pytest.mark.parametrize(
    "disks_paths,dev_indices,spare_indices,level,handles",
    (
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102"],
            [0, 1],
            [2],
            1,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102", "/dev/loop103"],
            [0, 1, 2],
            [3],
            5,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102"],
            [0, 1, 2],
            [],
            5,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
    ),
)
def test_BlockDevice_on_clean_drive(
    disks_paths,
    dev_indices,
    spare_indices,
    level,
    handles,
):
    handled_parts = HandledPartitions([setup_raid_disk(disks_paths)])
    handled_parts.create_partitions_mapping()
    handled_parts.commit_to_devices()
    try:
        handled_parts.commit_to_os()
    except IOException:
        LOGGER.warning(
            "Notifying the system directly failed. "
            "Program is probably not running as root. "
            "Attempting with sudo partx."
        )
    results = [
        subprocess.run(["sudo", "partx", "-u", disk.device.path], capture_output=True)
        for parter in handled_parts.partitionners
        for disk in parter.disks
    ]
    successes = [result.returncode == 0 for result in results]
    if not all(successes):
        raise IOError("Some modification on disks could not be notified to kernel")
    handled_parts.committed_to_os = True
    for index, handle in enumerate(handles):
        raid_volume = RaidVolume(
            raid_dev_name=f"/dev/md{100+index}",
            level=level,
            dev_indices=dev_indices,
            spare_indices=spare_indices,
            partitionners=handled_parts,
            partitions_handle=handle,
            handle=f"md{100+index}_{handle}",
        )
        assert not raid_volume.built
        raid_volume.build()
        assert raid_volume.built
    for index, handle in enumerate(handles):
        subprocess.run(["sudo", "mdadm", "--stop", "--force", f"/dev/md{100+index}"])
        parts: List[Partition] = list(
            handled_parts.partitionners[0].created_parttions_by_handle[handle]
        )
        parts_paths = [part.path for part in parts]
        subprocess.run(["sudo", "mdadm", "--zero-superblock"] + parts_paths)
        if os.path.exists(f"/dev/md{100+index}"):
            subprocess.run(
                ["sudo", "mdadm", "--stop", "--force", f"/dev/md{100+index}"]
            )
