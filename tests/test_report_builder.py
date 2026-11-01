import json
import re
import subprocess

import pytest
from device_guard import ensure_vg_absent
from device_helpers import make_parts

from py_part_recipe.fs import supported
from py_part_recipe.partition_common import (
    HandledPartitions,
    PartitionRequest,
    Partitionner,
    Recipe,
)
from py_part_recipe.report import Report, ReportError, VolumeReport
from py_part_recipe.report_builder import (
    FileSystemEntry,
    ReportCollectionError,
    build_report,
    parse_md_uuid,
    probe_tag,
)
from py_part_recipe.volume_common import (
    HandledVolumes,
    LvmLvVolume,
    LvmVgVolume,
    RaidVolume,
    RawVolume,
)

MIB = 1024 * 1024


# ----- reading the same values with other tools than the code under test


def ext_uuid(device: str) -> str:
    out = subprocess.run(["dumpe2fs", "-h", device], capture_output=True, check=True)
    match = re.search(r"Filesystem UUID:\s*(\S+)", out.stdout.decode("utf-8"))
    assert match, f"no uuid in dumpe2fs of {device}"
    return match.group(1)


def partition_uuid(disk: str, number: int) -> str:
    out = subprocess.run(
        ["sfdisk", "--part-uuid", disk, str(number)], capture_output=True, check=True
    )
    return out.stdout.decode("utf-8").strip().lower()


def array_uuid(device: str) -> str:
    out = subprocess.run(
        ["sudo", "mdadm", "--detail", device], capture_output=True, check=True
    )
    match = re.search(r"UUID : (\S+)", out.stdout.decode("utf-8"))
    assert match, f"no uuid in mdadm --detail {device}"
    return match.group(1)


def dm_uuid(name: str) -> str:
    out = subprocess.run(
        ["sudo", "dmsetup", "info", "-c", "--noheadings", "-o", "uuid", name],
        capture_output=True,
        check=True,
    )
    return out.stdout.decode("utf-8").strip()


# ----- parsing, no device needed


def test_parse_md_uuid():
    output = (
        "MD_LEVEL=raid1\nMD_DEVICES=2\nMD_UUID=5d0e1a2b:3c4d5e6f:70819203:a4b5c6d7\n"
    )
    assert parse_md_uuid(output) == "5d0e1a2b:3c4d5e6f:70819203:a4b5c6d7"


@pytest.mark.parametrize("output", ("", "MD_LEVEL=raid1\n", "MD_UUID=\n"))
def test_parse_md_uuid_missing(output):
    assert parse_md_uuid(output) is None


def test_file_system_of_an_unknown_volume():
    with pytest.raises(ReportCollectionError, match="nope.*no such volume"):
        build_report(
            HandledPartitions([]), HandledVolumes(), [FileSystemEntry("nope", "ext4")]
        )


# ----- partitions, raw volumes and file systems


@pytest.fixture
def raw_volumes(clean_disks):
    """Two partitions of 10 MiB on a disk, each one a raw volume."""
    requests = [
        PartitionRequest("boot_part", "10MiB", "10MiB", 100),
        PartitionRequest("data_part", "10MiB", "10MiB", 100),
    ]
    recipe = Recipe([clean_disks[0]], requests)
    handled = HandledPartitions([Partitionner(recipe)])
    make_parts(handled)
    volumes = HandledVolumes()
    for handle in ("boot", "data"):
        volumes.add(
            RawVolume(
                partitionners=handled,
                partitions_handle=f"{handle}_part",
                handle=f"volume_{handle}",
            )
        )
    return clean_disks[0], handled, volumes


def test_report_of_partitions_raw_volumes_and_file_systems(raw_volumes):
    disk, handled, volumes = raw_volumes
    boot = volumes.get_by_handle("volume_boot").sys_device
    data = volumes.get_by_handle("volume_data").sys_device
    supported["ext4"](boot, "boot disk").make_fs()
    supported["ext3"](data, "data").make_fs()
    entries = [
        FileSystemEntry("volume_boot", "ext4", mount="/boot"),
        FileSystemEntry("volume_data", "ext3", mount="/data", mount_options="noatime"),
    ]

    report = build_report(handled, volumes, entries)

    assert sorted(report.partitions) == ["boot_part", "data_part"]
    for number, handle in ((1, "boot_part"), (2, "data_part")):
        (partition,) = report.partitions[handle]
        assert partition.device == f"{disk}p{number}"
        assert partition.disk == disk
        assert partition.number == number
        assert partition.size_bytes == 10 * MIB
        assert partition.partuuid
        assert partition.partuuid.lower() == partition_uuid(disk, number)
    assert report.volumes["volume_boot"] == VolumeReport("raw", boot, None)
    boot_fs, data_fs = report.file_systems
    assert (boot_fs.device, boot_fs.type, boot_fs.formatted) == (boot, "ext4", True)
    assert boot_fs.uuid and boot_fs.uuid == ext_uuid(boot)
    assert boot_fs.label == "boot disk"
    assert (boot_fs.mount, boot_fs.mount_options, boot_fs.fsck_pass) == (
        "/boot",
        "defaults",
        2,
    )
    assert data_fs.uuid == ext_uuid(data)
    assert (data_fs.mount_options, data_fs.type) == ("noatime", "ext3")
    assert f"UUID={boot_fs.uuid}\t/boot\text4\tdefaults\t0\t2\n" in report.fstab()
    assert f"UUID={data_fs.uuid}\t/data\text3\tnoatime\t0\t2\n" in report.fstab()


def test_swap_has_a_uuid_and_no_mount_point(raw_volumes):
    _, handled, volumes = raw_volumes
    swap = volumes.get_by_handle("volume_data").sys_device
    supported["swap"](swap, "swap").make_fs()

    report = build_report(handled, volumes, [FileSystemEntry("volume_data", "swap")])

    (swap_fs,) = report.file_systems
    assert swap_fs.uuid
    assert (swap_fs.mount, swap_fs.mount_options, swap_fs.fsck_pass) == (None, "sw", 0)
    assert report.fstab() == f"UUID={swap_fs.uuid}\tnone\tswap\tsw\t0\t0\n"


@pytest.mark.parametrize("fmt", ("yaml", "json"))
def test_a_built_report_is_written_and_read_back(raw_volumes, fmt):
    _, handled, volumes = raw_volumes
    supported["ext4"](volumes.get_by_handle("volume_boot").sys_device, "boot").make_fs()
    report = build_report(
        handled, volumes, [FileSystemEntry("volume_boot", "ext4", mount="/")]
    )
    text = report.dumps(fmt)
    assert Report.loads(text) == report
    if fmt == "json":
        assert json.loads(text)["file_systems"][0]["mount"] == "/"


def test_file_system_not_formatted_by_the_recipe(raw_volumes):
    """The volume is left alone: the report keeps what is there, if anything."""
    _, handled, volumes = raw_volumes
    existing = volumes.get_by_handle("volume_data").sys_device
    supported["ext4"](existing, "kept").make_fs()
    entries = [
        FileSystemEntry("volume_boot", "ext4", formatted=False, mount="/boot"),
        FileSystemEntry("volume_data", "ext4", formatted=False, mount="/data"),
    ]

    report = build_report(handled, volumes, entries)

    empty, kept = report.file_systems
    assert (empty.formatted, empty.uuid, empty.label) == (False, None, None)
    assert (kept.formatted, kept.label) == (False, "kept")
    assert kept.uuid == ext_uuid(existing)
    # the fstab can not name a file system that has no uuid
    assert report.fstab() == f"UUID={kept.uuid}\t/data\text4\tdefaults\t0\t2\n"


def test_formatted_file_system_is_not_the_expected_one(raw_volumes):
    _, handled, volumes = raw_volumes
    supported["ext4"](volumes.get_by_handle("volume_boot").sys_device, "x").make_fs()
    with pytest.raises(ReportCollectionError, match="formatted as xfs.*ext4"):
        build_report(handled, volumes, [FileSystemEntry("volume_boot", "xfs")])


def test_formatted_volume_has_no_file_system(raw_volumes):
    _, handled, volumes = raw_volumes
    with pytest.raises(ReportCollectionError, match="no file system"):
        build_report(handled, volumes, [FileSystemEntry("volume_boot", "ext4")])


def test_two_file_systems_on_the_same_mount_point(raw_volumes):
    _, handled, volumes = raw_volumes
    for handle in ("volume_boot", "volume_data"):
        supported["ext4"](volumes.get_by_handle(handle).sys_device, "x").make_fs()
    entries = [
        FileSystemEntry("volume_boot", "ext4", mount="/data"),
        FileSystemEntry("volume_data", "ext4", mount="/data/"),
    ]
    with pytest.raises(ReportError, match="already used by"):
        build_report(handled, volumes, entries)


def test_probe_tag(raw_volumes):
    _, _, volumes = raw_volumes
    device = volumes.get_by_handle("volume_boot").sys_device
    assert probe_tag(device, "TYPE") is None
    supported["ext4"](device, "my boot label").make_fs()
    assert probe_tag(device, "TYPE") == "ext4"
    assert probe_tag(device, "LABEL") == "my boot label"
    assert probe_tag(device, "UUID") == ext_uuid(device)


# ----- raid and lvm


def test_report_of_a_raid_lvm_stack(clean_disks):
    """A logical volume with an ext4 on a volume group on a raid array."""
    ensure_vg_absent("vg_test")
    requests = [PartitionRequest("raid_part", "10MiB", "10MiB", 100, flags=["raid"])]
    handled = HandledPartitions([Partitionner(Recipe(clean_disks, requests))])
    make_parts(handled)
    volumes = HandledVolumes()
    volumes.add(
        RaidVolume(
            raid_dev_name="/dev/md100",
            level=1,
            dev_indices=[0, 1],
            partitionners=handled,
            partitions_handle="raid_part",
            handle="md100_vol",
        )
    )
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
    volumes.build()
    root = volumes.get_by_handle("lv_test").sys_device
    supported["ext4"](root, "root").make_fs()

    report = build_report(
        handled, volumes, [FileSystemEntry("lv_test", "ext4", mount="/")]
    )

    assert report.volumes["md100_vol"].type == "raid"
    assert report.volumes["md100_vol"].device == "/dev/md100"
    assert report.volumes["md100_vol"].uuid == array_uuid("/dev/md100")
    assert report.volumes["vg_test"].type == "volume_group"
    assert report.volumes["vg_test"].device == "/dev/vg_test"
    assert report.volumes["lv_test"].type == "logical_volume"
    assert report.volumes["lv_test"].device == root
    # device mapper gives "LVM-" followed by the uuids of the volume group and
    # of the logical volume, without their dashes
    vg_uuid = report.volumes["vg_test"].uuid
    lv_uuid = report.volumes["lv_test"].uuid
    assert vg_uuid and lv_uuid
    assert dm_uuid("vg_test-lv_test") == "LVM-" + (vg_uuid + lv_uuid).replace("-", "")
    (fs,) = report.file_systems
    assert fs.uuid == ext_uuid(root)
    assert len(report.partitions["raid_part"]) == 2
    assert report.fstab() == f"UUID={fs.uuid}\t/\text4\tdefaults\t0\t1\n"
