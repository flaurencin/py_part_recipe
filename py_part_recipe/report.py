"""The report written after a recipe is applied.

The report describes what was created: partitions, volumes and file systems,
with their devices and UUIDs. It is the only thing the other commands need:
mounting, unmounting and building an fstab all read a report, not the recipe.

A report is a plain dict, written as yaml or json (json is read by the yaml
parser, so both can be loaded the same way)::

    version: 1
    partitions:
      <partition handle>:
        - {device, disk, number, partuuid, size_bytes}
    volumes:
      <volume handle>: {type, device, uuid}
    file_systems:
      - {volume, type, formatted, device, uuid, label, mount,
         mount_options, fsck_pass}

`mount` is the mount point in the installed system (`/`, `/boot`, ...), the
root directory where it is mounted is chosen when mounting. Swap has no mount
point and is never activated.
"""

import json
import re
from dataclasses import MISSING, dataclass, field, fields
from logging import getLogger
from typing import Any

import yaml

LOGGER = getLogger(__name__)

REPORT_VERSION = 1

# file system types a report can describe, and the type fstab and mount use
FSTAB_TYPES = {
    "ext2": "ext2",
    "ext3": "ext3",
    "ext4": "ext4",
    "fat16": "vfat",
    "fat32": "vfat",
    "xfs": "xfs",
    "btrfs": "btrfs",
    "exfat": "exfat",
    "ntfs": "ntfs-3g",
    "swap": "swap",
}
# file systems checked by fsck at boot: 1 for the root, 2 for the others
FSCK_TYPES = ("ext2", "ext3", "ext4", "fat16", "fat32")
MOUNT_RE = re.compile(r"^/[^\s]*$")


class ReportError(ValueError):
    """The report is not valid, `errors` has every problem found."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


def normalize_mount(mount: str) -> str:
    """`//var/./log/` -> `/var/log`. Only for a valid absolute mount point."""
    parts = [part for part in mount.split("/") if part not in ("", ".")]
    return "/" + "/".join(parts)


def default_fsck_pass(fs_type: str, mount: str | None) -> int:
    if fs_type not in FSCK_TYPES or not mount:
        return 0
    return 1 if mount == "/" else 2


@dataclass
class PartitionReport:
    device: str
    disk: str
    number: int
    partuuid: str | None
    size_bytes: int


@dataclass
class VolumeReport:
    type: str
    device: str
    uuid: str | None


@dataclass
class FileSystemReport:
    volume: str
    type: str
    formatted: bool
    device: str
    uuid: str | None
    label: str | None = None
    mount: str | None = None
    mount_options: str | None = None
    fsck_pass: int | None = None

    def __post_init__(self):
        if (
            isinstance(self.mount, str)
            and MOUNT_RE.match(self.mount)
            and ".." not in self.mount.split("/")
        ):
            self.mount = normalize_mount(self.mount)
        # explicit values in the report, whoever reads it needs no defaults
        if self.mount_options is None:
            self.mount_options = "sw" if self.type == "swap" else "defaults"
        if self.fsck_pass is None:
            self.fsck_pass = default_fsck_pass(self.type, self.mount)

    @property
    def fstab_type(self) -> str:
        return FSTAB_TYPES[self.type]


def _check_keys(data: Any, cls: type, where: str, errors: list[str]) -> bool:
    """Check that data is a dict with the fields of the class, no more, no less
    (fields having a default may be missing)."""
    if not isinstance(data, dict):
        errors.append(f"{where}: expected a mapping, got {type(data).__name__}")
        return False
    ok = True
    names = {f.name for f in fields(cls)}
    required = {
        f.name
        for f in fields(cls)
        if f.default is MISSING and f.default_factory is MISSING
    }
    for key in data:
        if key not in names:
            errors.append(f"{where}: unknown key '{key}'")
            ok = False
    for name in sorted(required - set(data)):
        errors.append(f"{where}: missing key '{name}'")
        ok = False
    return ok


def _check_type(value: Any, expected: type | tuple, where: str, errors: list[str]):
    # bool is an int in python, a number of partitions can not be True
    if isinstance(value, bool) and expected not in (bool, (bool,)):
        errors.append(f"{where}: unexpected boolean")
        return False
    if not isinstance(value, expected):
        names = (
            "/".join(t.__name__ for t in expected)
            if isinstance(expected, tuple)
            else expected.__name__
        )
        errors.append(f"{where}: expected {names}, got {type(value).__name__}")
        return False
    return True


@dataclass
class Report:
    partitions: dict[str, list[PartitionReport]] = field(default_factory=dict)
    volumes: dict[str, VolumeReport] = field(default_factory=dict)
    file_systems: list[FileSystemReport] = field(default_factory=list)
    version: int = REPORT_VERSION

    # ----- validation

    def problems(self) -> list[str]:
        """Every inconsistency of the report, empty when it is valid."""
        errors: list[str] = []
        if self.version != REPORT_VERSION:
            errors.append(
                f"version {self.version} is not supported, expected {REPORT_VERSION}"
            )
        mounts: dict[str, str] = {}
        for index, fs in enumerate(self.file_systems):
            where = f"file_systems[{index}] ({fs.volume})"
            if fs.volume not in self.volumes:
                errors.append(f"{where}: unknown volume '{fs.volume}'")
            if fs.type not in FSTAB_TYPES:
                errors.append(f"{where}: unknown file system type '{fs.type}'")
            if fs.type == "swap" and fs.mount is not None:
                errors.append(f"{where}: swap has no mount point, got '{fs.mount}'")
            if fs.mount is not None:
                if not MOUNT_RE.match(fs.mount) or ".." in fs.mount.split("/"):
                    errors.append(
                        f"{where}: mount must be an absolute path without "
                        f"spaces or '..', got '{fs.mount}'"
                    )
                else:
                    mount = fs.mount
                    if mount in mounts:
                        errors.append(
                            f"{where}: mount '{mount}' is already used by "
                            f"{mounts[mount]}"
                        )
                    mounts[mount] = fs.volume
            if fs.fsck_pass not in (0, 1, 2):
                errors.append(f"{where}: fsck_pass must be 0, 1 or 2")
        return errors

    def validate(self):
        errors = self.problems()
        if errors:
            raise ReportError(errors)

    # ----- reading and writing

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "partitions": {
                handle: [vars(part).copy() for part in parts]
                for handle, parts in self.partitions.items()
            },
            "volumes": {
                handle: vars(volume).copy() for handle, volume in self.volumes.items()
            },
            "file_systems": [vars(fs).copy() for fs in self.file_systems],
        }

    @classmethod
    def from_dict(cls, data: Any) -> "Report":
        """Build a validated report, raises ReportError listing every problem."""
        errors: list[str] = []
        if not isinstance(data, dict):
            raise ReportError(
                [f"report: expected a mapping, got {type(data).__name__}"]
            )
        for key in data:
            if key not in ("version", "partitions", "volumes", "file_systems"):
                errors.append(f"report: unknown key '{key}'")
        for key in ("version", "partitions", "volumes", "file_systems"):
            if key not in data:
                errors.append(f"report: missing key '{key}'")
        if errors:
            raise ReportError(errors)
        if not _check_type(data["version"], int, "version", errors):
            raise ReportError(errors)
        report = cls(version=data["version"])

        if _check_type(data["partitions"], dict, "partitions", errors):
            for handle, parts in data["partitions"].items():
                where = f"partitions.{handle}"
                if not _check_type(parts, list, where, errors):
                    continue
                for index, part in enumerate(parts):
                    item = f"{where}[{index}]"
                    if _check_keys(part, PartitionReport, item, errors):
                        _check_item_types(
                            part,
                            item,
                            errors,
                            device=str,
                            disk=str,
                            number=int,
                            partuuid=(str, type(None)),
                            size_bytes=int,
                        )
                        report.partitions.setdefault(handle, []).append(
                            PartitionReport(**part)
                        )
        if _check_type(data["volumes"], dict, "volumes", errors):
            for handle, volume in data["volumes"].items():
                item = f"volumes.{handle}"
                if _check_keys(volume, VolumeReport, item, errors):
                    _check_item_types(
                        volume,
                        item,
                        errors,
                        type=str,
                        device=str,
                        uuid=(str, type(None)),
                    )
                    report.volumes[handle] = VolumeReport(**volume)
        if _check_type(data["file_systems"], list, "file_systems", errors):
            for index, fs in enumerate(data["file_systems"]):
                item = f"file_systems[{index}]"
                if _check_keys(fs, FileSystemReport, item, errors):
                    _check_item_types(
                        fs,
                        item,
                        errors,
                        volume=str,
                        type=str,
                        formatted=bool,
                        device=str,
                        uuid=(str, type(None)),
                        label=(str, type(None)),
                        mount=(str, type(None)),
                        mount_options=(str, type(None)),
                        fsck_pass=(int, type(None)),
                    )
                    report.file_systems.append(FileSystemReport(**fs))
        if errors:
            raise ReportError(errors)
        report.validate()
        return report

    def dumps(self, fmt: str = "yaml") -> str:
        if fmt == "yaml":
            return yaml.safe_dump(
                self.to_dict(), default_flow_style=False, sort_keys=False
            )
        if fmt == "json":
            return json.dumps(self.to_dict(), indent=4) + "\n"
        raise ValueError(f"unknown report format '{fmt}', use yaml or json")

    @classmethod
    def loads(cls, text: str) -> "Report":
        """Read a report written as yaml or json."""
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise ReportError([f"report is not valid yaml or json: {error}"]) from error
        return cls.from_dict(data)

    def dump(self, path: str, fmt: str | None = None):
        """Write the report, the format defaults to the extension (.json)."""
        if fmt is None:
            fmt = "json" if path.endswith(".json") else "yaml"
        with open(path, "w", encoding="utf-8") as report_file:
            report_file.write(self.dumps(fmt))

    @classmethod
    def load(cls, path: str) -> "Report":
        with open(path, encoding="utf-8") as report_file:
            return cls.loads(report_file.read())

    # ----- views

    def mounted_file_systems(self) -> list[FileSystemReport]:
        """File systems having a mount point, shortest first.

        A path always contains its parent, so a parent is always before its
        children: this is the order to mount, and the reverse to unmount.
        """
        mounted = [fs for fs in self.file_systems if fs.mount is not None]
        return sorted(mounted, key=lambda fs: (len(fs.mount), fs.mount))

    def swap_file_systems(self) -> list[FileSystemReport]:
        return [fs for fs in self.file_systems if fs.type == "swap"]

    def fstab(self) -> str:
        """The content of an fstab file, devices are referenced by UUID.

        A file system without UUID can not be referenced reliably: it is left
        out and a warning is logged.
        """
        lines = []
        for fs in self.mounted_file_systems() + self.swap_file_systems():
            if not fs.uuid:
                LOGGER.warning(
                    "No UUID for the file system of volume %s, it is not in the fstab",
                    fs.volume,
                )
                continue
            mount = fs.mount if fs.mount is not None else "none"
            lines.append(
                f"UUID={fs.uuid}\t{mount}\t{fs.fstab_type}\t{fs.mount_options}"
                f"\t0\t{fs.fsck_pass}"
            )
        return "\n".join(lines) + ("\n" if lines else "")


def _check_item_types(data: dict, where: str, errors: list[str], **expected):
    for name, expected_type in expected.items():
        if name in data:
            _check_type(data[name], expected_type, f"{where}.{name}", errors)
