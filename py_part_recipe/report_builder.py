"""Build the report of what a recipe created, from the system itself.

Nothing is taken from what was asked: the devices, UUIDs and labels are read
from the system once everything is built, so the report tells what exists.
UUIDs come from `blkid` (file systems and partitions), `mdadm` (RAID arrays)
and the lvm tools (volume groups and logical volumes).
"""

from dataclasses import dataclass
import subprocess
from typing import Optional

from py_part_recipe.common import gen_cmd_for_subprocess
from py_part_recipe.partition_common import HandledPartitions
from py_part_recipe.report import (
    FileSystemReport,
    PartitionReport,
    Report,
    VolumeReport,
)
from py_part_recipe.volume_common import (
    HandledVolumes,
    LvmLvVolume,
    LvmVgVolume,
    RaidVolume,
    RawVolume,
    Volume,
)

VOLUME_TYPES = (
    (RawVolume, "raw"),
    (RaidVolume, "raid"),
    (LvmVgVolume, "volume_group"),
    (LvmLvVolume, "logical_volume"),
)
# what blkid calls the file systems of a report
BLKID_TYPES = {
    "ext2": "ext2",
    "ext3": "ext3",
    "ext4": "ext4",
    "fat16": "vfat",
    "fat32": "vfat",
    "xfs": "xfs",
    "btrfs": "btrfs",
    "exfat": "exfat",
    "ntfs": "ntfs",
    "swap": "swap",
}


class ReportCollectionError(Exception): ...


@dataclass
class FileSystemEntry:
    """A file system of the recipe, on a volume (by handle).

    `formatted` tells if the recipe formatted the volume. When it did not, the
    volume is left untouched: the report keeps what is found on it, if
    anything.
    """

    volume: str
    type: str
    formatted: bool = True
    mount: Optional[str] = None
    mount_options: Optional[str] = None
    fsck_pass: Optional[int] = None


def _output(command: list[str], what: str) -> str:
    result = subprocess.run(gen_cmd_for_subprocess(command), capture_output=True)
    if result.returncode != 0:
        error = result.stderr.decode("utf-8", errors="replace").strip()
        raise ReportCollectionError(
            f"Could not read {what}. Command: {' '.join(command)} "
            f"Failed ({result.returncode}). {error}"
        )
    return result.stdout.decode("utf-8")


def probe_tag(device: str, tag: str) -> Optional[str]:
    """Read a tag (TYPE, UUID, LABEL, PART_ENTRY_UUID...) written on a device.

    blkid probes the device itself, not a cache that could be outdated.
    None when the device has no such tag.
    """
    command = ["blkid", "-p", "-s", tag, "-o", "value", device]
    result = subprocess.run(gen_cmd_for_subprocess(command), capture_output=True)
    if result.returncode == 2:  # nothing found
        return None
    if result.returncode != 0:
        error = result.stderr.decode("utf-8", errors="replace").strip()
        raise ReportCollectionError(
            f"Could not read {tag} of {device}. Command: {' '.join(command)} "
            f"Failed ({result.returncode}). {error}"
        )
    return result.stdout.decode("utf-8").strip() or None


def parse_md_uuid(detail_export: str) -> Optional[str]:
    """MD_UUID of the output of `mdadm --detail --export`."""
    for line in detail_export.splitlines():
        key, _, value = line.partition("=")
        if key.strip() == "MD_UUID":
            return value.strip() or None
    return None


def md_uuid(device: str) -> Optional[str]:
    output = _output(["mdadm", "--detail", "--export", device], f"array {device}")
    return parse_md_uuid(output)


def lvm_uuid(command: str, field: str, name: str) -> Optional[str]:
    """Read a uuid with the lvm reports: ("vgs", "vg_uuid", "vg") or
    ("lvs", "lv_uuid", "vg/lv")."""
    output = _output([command, "--noheadings", "-o", field, name], f"{field} of {name}")
    return output.strip() or None


def _partition_report(partition) -> PartitionReport:
    device = partition.disk.device
    return PartitionReport(
        device=partition.path,
        disk=device.path,
        number=partition.number,
        partuuid=probe_tag(partition.path, "PART_ENTRY_UUID"),
        size_bytes=partition.geometry.length * device.sectorSize,
    )


def _volume_report(volume: Volume) -> VolumeReport:
    device = volume.sys_device
    for volume_class, type_name in VOLUME_TYPES:
        if isinstance(volume, volume_class):
            break
    else:
        raise ReportCollectionError(
            f"Volume {volume.handle} has an unknown type {type(volume).__name__}"
        )
    uuid = None
    if isinstance(volume, RaidVolume):
        uuid = md_uuid(device)
    elif isinstance(volume, LvmVgVolume):
        uuid = lvm_uuid("vgs", "vg_uuid", volume.handle)
    elif isinstance(volume, LvmLvVolume):
        vg_name = volume.get_volume_by_handle().handle
        uuid = lvm_uuid("lvs", "lv_uuid", f"{vg_name}/{volume.handle}")
    return VolumeReport(type=type_name, device=device, uuid=uuid)


def _file_system_report(entry: FileSystemEntry, volume: Volume) -> FileSystemReport:
    device = volume.sys_device
    found_type = probe_tag(device, "TYPE")
    expected = BLKID_TYPES.get(entry.type)
    if entry.formatted and found_type != expected:
        raise ReportCollectionError(
            f"Volume {entry.volume} was formatted as {entry.type} but "
            f"{device} holds {found_type or 'no file system'}"
        )
    return FileSystemReport(
        volume=entry.volume,
        type=entry.type,
        formatted=entry.formatted,
        device=device,
        uuid=probe_tag(device, "UUID"),
        label=probe_tag(device, "LABEL"),
        mount=entry.mount,
        mount_options=entry.mount_options,
        fsck_pass=entry.fsck_pass,
    )


def build_report(
    partitions: HandledPartitions,
    volumes: HandledVolumes,
    file_systems: list[FileSystemEntry],
) -> Report:
    """Read the system and describe what the recipe created.

    Call it once everything is built: the partitions are known by the kernel,
    the volumes are built and the file systems are formatted.

    Raises:
        ReportCollectionError: something could not be read, a volume is
            missing, or a file system is not the one that was formatted
        ReportError: the report is inconsistent, like two file systems with
            the same mount point
    """
    report = Report()
    for handle, parts in partitions.partitions_by_handle().items():
        report.partitions[handle] = [_partition_report(part) for part in parts]
    for handle, volume in volumes.volumes.items():
        report.volumes[handle] = _volume_report(volume)
    for entry in file_systems:
        if entry.volume not in volumes.volumes:
            raise ReportCollectionError(
                f"The file system of volume {entry.volume} has no such volume"
            )
        volume = volumes.get_by_handle(entry.volume)
        report.file_systems.append(_file_system_report(entry, volume))
    report.validate()
    return report
