import os
import subprocess

import pytest
from device_guard import ensure_vg_absent
from device_helpers import make_parts, signature_present, signatures, write_at

from py_part_recipe.fs import supported
from py_part_recipe.partition_common import (
    HandledPartitions,
    PartitionRequest,
    Partitionner,
    Recipe,
)
from py_part_recipe.volume_common import (
    HandledVolumes,
    LvmLvVolume,
    LvmVgVolume,
    RaidVolume,
)
from py_part_recipe.wipe import (
    WipeError,
    holders_of,
    partitions_of,
    wipe_disk,
    wipe_signatures,
)

MIB = 1024 * 1024
FS_DEVICE = "/dev/loop106"


def zero(device: str, size: int = 4 * MIB):
    write_at(device, 0, b"\0" * size)


def layout(devices, wipe: bool, requests=None) -> HandledPartitions:
    requests = requests or [PartitionRequest("data", "10MiB", "10MiB", 100)]
    return HandledPartitions([Partitionner(Recipe(devices, requests, wipe=wipe))])


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
def ext4_image(tmp_path):
    """The bytes of a small ext4 file system, made in a regular file."""
    image = tmp_path / "ext4.img"
    subprocess.run(["truncate", "-s", "8M", str(image)], check=True)
    subprocess.run(["mkfs.ext4", "-q", str(image)], check=True)
    return image.read_bytes()


# ----- erasing signatures


@pytest.mark.parametrize("fs_type", ("ext4", "fat32", "xfs", "btrfs", "swap"))
def test_wipe_signatures_removes_file_system(fs_type):
    zero(FS_DEVICE, 1 * MIB)
    supported[fs_type](FS_DEVICE, "wipe").make_fs()
    assert signature_present(FS_DEVICE)
    wipe_signatures(FS_DEVICE)
    assert signatures(FS_DEVICE) == {}


def test_wipe_signatures_failure_is_reported():
    with pytest.raises(WipeError, match="wipefs"):
        wipe_signatures("/dev/md199")


# ----- wiping a disk


def test_wipe_disk_unknown_device():
    with pytest.raises(ValueError, match="not a block device"):
        wipe_disk("/dev/loop107")


def test_recipe_wipe_and_keep_partitions_are_exclusive():
    with pytest.raises(ValueError, match="keep_partitions"):
        Recipe(["/dev/loop100"], wipe=True, keep_partitions=True)


def test_wipe_disk_removes_partitions_and_table(clean_disks):
    disk = clean_disks[0]
    handled = layout([disk], wipe=False)
    make_parts(handled)
    partition = handled.get_partitions_by_handle("data")[0]
    supported["ext4"](partition.path, "old").make_fs()
    assert signature_present(disk) and signature_present(partition.path)
    assert partitions_of("loop100") == ["loop100p1"]

    wipe_disk(disk)

    assert signatures(disk) == {}
    assert partitions_of("loop100") == []


def test_wipe_disk_refuses_a_partition(clean_disks):
    handled = layout([clean_disks[0]], wipe=False)
    make_parts(handled)
    with pytest.raises(ValueError, match="is a partition"):
        wipe_disk(handled.get_partitions_by_handle("data")[0].path)


# ----- old signatures inside the new partitions

# the first partition starts at sector 2048, 1 MiB from the start of the disk
FIRST_PARTITION_START = 1 * MIB


def test_old_signature_survives_without_wipe(clean_disks, ext4_image):
    """What wipe is for: the old file system shows up in the new partition."""
    disk = clean_disks[0]
    write_at(disk, FIRST_PARTITION_START, ext4_image)
    handled = layout([disk], wipe=False)
    make_parts(handled)
    handled.wipe_partitions()  # does nothing, the recipe does not ask to wipe
    assert signature_present(handled.get_partitions_by_handle("data")[0].path)


def test_wipe_removes_old_signature_of_new_partitions(clean_disks, ext4_image):
    disk = clean_disks[0]
    write_at(disk, FIRST_PARTITION_START, ext4_image)
    handled = layout([disk], wipe=True)
    make_parts(handled)
    partition = handled.get_partitions_by_handle("data")[0].path
    # the disk had no partition, wiping the disk did not see that file system
    assert signature_present(partition)

    handled.wipe_partitions()

    assert signatures(partition) == {}


# ----- releasing what is built on the disk


def raid_requests():
    return [PartitionRequest("raid_part", "10MiB", "10MiB", 100, flags=["raid"])]


def raid_volume(handled: HandledPartitions) -> RaidVolume:
    return RaidVolume(
        raid_dev_name="/dev/md100",
        level=1,
        dev_indices=[0, 1],
        partitionners=handled,
        partitions_handle="raid_part",
        handle="md100_vol",
    )


def test_wipe_disk_stops_raid_array(clean_disks):
    handled = layout(clean_disks, wipe=False, requests=raid_requests())
    make_parts(handled)
    raid = raid_volume(handled)
    raid.build()
    assert os.path.exists("/dev/md100")
    assert holders_of("loop100p1") == ["md100"]
    try:
        wipe_disk(clean_disks[0])
        assert not os.path.exists("/dev/md100")
        assert holders_of("loop101p1") == []
    finally:
        subprocess.run(["sudo", "mdadm", "--stop", "/dev/md100"], capture_output=True)


def test_wipe_disk_stops_a_stack_raid_lvm(clean_disks):
    """A logical volume on a volume group on a raid array on the disks."""
    ensure_vg_absent("vg_test")
    handled = layout(clean_disks, wipe=False, requests=raid_requests())
    make_parts(handled)
    volumes = HandledVolumes()
    volumes.add(raid_volume(handled))
    volumes.add(
        LvmVgVolume(
            handled_parts=handled,
            partitions_handles=[],
            handled_vols=volumes,
            volumes_handles=["md100_vol"],
            handle="vg_test",
        )
    )
    volumes.add(
        LvmLvVolume(
            handled_vols=volumes,
            volume_handle="vg_test",
            handle="lv_test",
            vg_percent=50,
        )
    )
    try:
        volumes.build()
        assert os.path.exists("/dev/mapper/vg_test-lv_test")
        assert os.path.exists("/dev/md100")

        wipe_disk(clean_disks[0])

        assert not os.path.exists("/dev/mapper/vg_test-lv_test")
        assert not os.path.exists("/dev/md100")
    finally:
        subprocess.run(["sudo", "mdadm", "--stop", "/dev/md100"], capture_output=True)
