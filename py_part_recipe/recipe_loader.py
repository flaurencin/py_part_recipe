"""Read and validate a recipe, without touching any disk.

A recipe describes, from the disks to the file systems::

    partitioning:               # partitions created on groups of disks
      - devices: [/dev/sda, /dev/sdb]
        recipe:
          keep_partitions: false    # optional, add to the free space
          wipe: false               # optional, erase the disks first
          partitions:
            - {handle, min_size, max_size, weight, p_type, flags}
    volumes:                    # built on partitions or on other volumes
      - {handle, type: raw | raid | volume_group | logical_volume, params}
    file_systems:               # formatted on volumes
      - {type, volume_handle, format, label, mount, mount_options, fsck_pass}

Everything that can be checked from the recipe alone is checked here, all the
problems are reported at once (RecipeError), so that nothing is started on a
recipe that can not succeed: sizes and flags, handles that must exist and be
defined before they are used, a partition or volume used twice, raid devices
against the number of disks, logical volumes against the size of their volume
group, mount points. What depends on the disks (is there enough space?) can
only be checked when the recipe is applied.

`mount` is the mount point in the installed system (`/`, `/boot`, ...), the
directory where it is mounted is chosen when mounting.
"""

from collections import defaultdict
from dataclasses import dataclass, field
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from py_part_recipe.common import validate_handle
from py_part_recipe.partition_common import PartitionRequest
from py_part_recipe.report import FSTAB_TYPES, MOUNT_RE, normalize_mount
from py_part_recipe.volume_common import (
    RAID_LEVELS,
    RAID_METADATA_VERSIONS,
    check_raid_layout,
)

VOLUME_TYPES = ("raw", "raid", "volume_group", "logical_volume")
# volumes that can get a file system (a volume group holds logical volumes)
FORMATTABLE_VOLUME_TYPES = ("raw", "raid", "logical_volume")
RAID_DEVICE_NAME_RE = re.compile(r"^/dev/md[0-9]+$")
OPTIONS_RE = re.compile(r"^\S+$")
# a hint when a key of an older version of the format is found
KEY_HINTS = {
    "install_mount": "use 'mount', the path in the installed system (/, /boot...)",
}


class RecipeError(ValueError):
    """The recipe is not valid, `errors` has every problem found."""

    def __init__(self, errors: List[str]) -> None:
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


# ----- what a valid recipe is made of


@dataclass
class PartitionGroupSpec:
    devices: List[str]
    partitions: List[PartitionRequest]
    keep_partitions: bool = False
    wipe: bool = False


@dataclass
class RawParams:
    partitions_handle: str


@dataclass
class RaidParams:
    partitions_handle: str
    level: int
    dev_indices: List[int]
    raid_dev_name: str
    spare_indices: List[int] = field(default_factory=list)
    meta_data: str = "1.2"


@dataclass
class VolumeGroupParams:
    partitions_handles: List[str] = field(default_factory=list)
    volumes_handles: List[str] = field(default_factory=list)


@dataclass
class LogicalVolumeParams:
    volume_handle: str
    vg_percent: float


VolumeParams = Union[RawParams, RaidParams, VolumeGroupParams, LogicalVolumeParams]


@dataclass
class VolumeSpec:
    handle: str
    type: str
    params: VolumeParams


@dataclass
class FileSystemSpec:
    type: str
    volume_handle: str
    format: bool = True
    label: Optional[str] = None
    mount: Optional[str] = None
    mount_options: Optional[str] = None
    fsck_pass: Optional[int] = None


@dataclass
class RecipeSpec:
    partitioning: List[PartitionGroupSpec]
    volumes: List[VolumeSpec] = field(default_factory=list)
    file_systems: List[FileSystemSpec] = field(default_factory=list)


# ----- checking the types of what was read


class _Checker:
    """Collect the problems, say where each one is."""

    def __init__(self) -> None:
        self.errors: List[str] = []

    def add(self, where: str, message: str):
        self.errors.append(f"{where}: {message}")

    def mapping(
        self,
        data: Any,
        where: str,
        required: Tuple[str, ...] = (),
        optional: Tuple[str, ...] = (),
    ) -> Optional[dict]:
        """The data if it is a mapping with the expected keys, else None."""
        if not isinstance(data, dict):
            self.add(where, f"expected a mapping, got {type(data).__name__}")
            return None
        ok = True
        for key in data:
            if key not in required and key not in optional:
                hint = f" ({KEY_HINTS[key]})" if key in KEY_HINTS else ""
                self.add(where, f"unknown key '{key}'{hint}")
                ok = False
        for key in required:
            if key not in data:
                self.add(where, f"missing key '{key}'")
                ok = False
        return data if ok else None

    def string(
        self, value: Any, where: str, allow_empty: bool = False
    ) -> Optional[str]:
        if not isinstance(value, str) or not (value or allow_empty):
            self.add(where, f"expected a non empty string, got {value!r}")
            return None
        return value

    def boolean(self, value: Any, where: str, default: bool = False) -> bool:
        if value is None:  # a key left empty in the yaml
            return default
        if not isinstance(value, bool):
            self.add(where, f"expected true or false, got {value!r}")
            return default
        return value

    def integer(self, value: Any, where: str) -> Optional[int]:
        if isinstance(value, bool) or not isinstance(value, int):
            self.add(where, f"expected an integer, got {value!r}")
            return None
        return value

    def sequence(
        self, value: Any, where: str, allow_empty: bool = False
    ) -> Optional[list]:
        if not isinstance(value, list) or not (value or allow_empty):
            self.add(where, f"expected a non empty list, got {value!r}")
            return None
        return value

    def strings(self, value: Any, where: str, default_empty: bool = True) -> List[str]:
        """A list of strings; a missing or empty value is an empty list."""
        if value is None and default_empty:
            return []
        items = self.sequence(value, where, allow_empty=True)
        if items is None:
            return []
        return [
            item
            for index, item in enumerate(items)
            if self.string(item, f"{where}[{index}]") is not None
        ]

    def handle(self, value: Any, where: str) -> Optional[str]:
        text = self.string(value, where)
        if text is None:
            return None
        try:
            return validate_handle(text)
        except ValueError as error:
            self.add(where, str(error))
            return None


# ----- partitioning


def _partition_request(
    checker: _Checker, item: Any, where: str
) -> Optional[PartitionRequest]:
    data = checker.mapping(
        item,
        where,
        required=("handle", "min_size", "max_size", "weight"),
        optional=("p_type", "flags"),
    )
    if data is None:
        return None
    handle = checker.handle(data["handle"], f"{where}.handle")
    sizes = {}
    for key in ("min_size", "max_size"):
        value = data[key]
        if isinstance(value, bool) or not isinstance(value, (str, int)) or value == "":
            checker.add(f"{where}.{key}", f"expected a size, got {value!r}")
        else:
            sizes[key] = str(value)
    weight = checker.integer(data["weight"], f"{where}.weight")
    if weight is not None and weight < 1:
        checker.add(f"{where}.weight", f"must be at least 1, got {weight}")
        weight = None
    p_type = data.get("p_type")
    if p_type is None:
        p_type = "normal"
    flags = checker.strings(data.get("flags"), f"{where}.flags")
    if handle is None or len(sizes) != 2 or weight is None:
        return None
    try:
        request = PartitionRequest(
            handle=handle,
            min_size=sizes["min_size"],
            max_size=sizes["max_size"],
            weight=weight,
            p_type=p_type,
            flags=list(flags),
        )
    except (ValueError, KeyError, TypeError) as error:
        checker.add(where, str(error).strip("'\""))
        return None
    if request.min_size_bytes < 1:
        checker.add(f"{where}.min_size", "must be more than 0")
    if request.min_size_bytes > request.max_size_bytes:
        checker.add(where, "min_size is bigger than max_size")
    return request


def _handle_of(item: Any) -> Optional[str]:
    """The handle of a partition whatever else is wrong with it, if it is valid."""
    handle = item.get("handle") if isinstance(item, dict) else None
    try:
        return validate_handle(handle) if isinstance(handle, str) else None
    except ValueError:
        return None


def _partitioning(
    checker: _Checker, data: Any
) -> Tuple[List[PartitionGroupSpec], Dict[str, int]]:
    """The groups, and the number of partitions of each handle (one per disk)."""
    groups: List[PartitionGroupSpec] = []
    partitions_per_handle: Dict[str, int] = {}
    items = checker.sequence(data, "partitioning")
    seen_devices: Dict[str, int] = {}
    for index, item in enumerate(items or []):
        where = f"partitioning[{index}]"
        group = checker.mapping(item, where, required=("devices", "recipe"))
        if group is None:
            continue
        devices = checker.strings(group["devices"], f"{where}.devices", False)
        if not devices:
            checker.add(f"{where}.devices", "at least one device is needed")
        for dev_index, device in enumerate(devices):
            if not device.startswith("/dev/"):
                checker.add(
                    f"{where}.devices[{dev_index}]", f"{device} is not under /dev/"
                )
            if device in seen_devices:
                checker.add(
                    f"{where}.devices[{dev_index}]",
                    (
                        f"{device} is already used by partitioning[{seen_devices[device]}]"
                        if seen_devices[device] != index
                        else f"{device} is given twice"
                    ),
                )
            seen_devices[device] = index
        recipe = checker.mapping(
            group["recipe"],
            f"{where}.recipe",
            required=("partitions",),
            optional=("keep_partitions", "wipe"),
        )
        if recipe is None:
            continue
        keep = checker.boolean(
            recipe.get("keep_partitions"), f"{where}.recipe.keep_partitions"
        )
        wipe = checker.boolean(recipe.get("wipe"), f"{where}.recipe.wipe")
        if keep and wipe:
            checker.add(
                f"{where}.recipe", "wipe erases the disks, it can not keep partitions"
            )
        requests = []
        items_p = checker.sequence(recipe["partitions"], f"{where}.recipe.partitions")
        for part_index, part in enumerate(items_p or []):
            part_where = f"{where}.recipe.partitions[{part_index}]"
            request = _partition_request(checker, part, part_where)
            # known from now on, even if its other fields are wrong, so that
            # the volumes using it do not report a second problem
            handle = request.handle if request else _handle_of(part)
            if handle is None:
                continue
            if handle in partitions_per_handle:
                checker.add(part_where, f"handle {handle} is used twice")
                continue
            partitions_per_handle[handle] = len(set(devices))
            if request is not None:
                requests.append(request)
        groups.append(PartitionGroupSpec(list(devices), requests, keep, wipe))
    return groups, partitions_per_handle


# ----- volumes


@dataclass
class _VolumeContext:
    """What the volumes read and fill while they are checked, in order."""

    partitions_per_handle: Dict[str, int]
    types: Dict[str, str] = field(default_factory=dict)  # volumes defined so far
    partition_users: Dict[str, str] = field(default_factory=dict)
    volume_users: Dict[str, str] = field(default_factory=dict)
    raid_names: Dict[str, str] = field(default_factory=dict)
    percent_per_group: Dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )


def _use_partitions(
    checker: _Checker,
    context: _VolumeContext,
    handles: List[str],
    user: str,
    where: str,
):
    for handle in handles:
        if handle not in context.partitions_per_handle:
            checker.add(where, f"unknown partitions handle '{handle}'")
        elif handle in context.partition_users:
            checker.add(
                where,
                f"partitions '{handle}' are already used by volume "
                f"'{context.partition_users[handle]}'",
            )
        else:
            context.partition_users[handle] = user


def _use_volume(
    checker: _Checker,
    context: _VolumeContext,
    handle: str,
    user: str,
    where: str,
    wanted_type: Optional[str] = None,
    exclusive: bool = True,
):
    """Use a volume, defined before. A volume group is shared by its logical
    volumes (not exclusive), any other volume has one user only."""
    if handle not in context.types:
        checker.add(
            where,
            f"unknown volume '{handle}' (a volume must be defined before the "
            "volumes that use it)",
        )
        return
    if wanted_type and context.types[handle] != wanted_type:
        checker.add(
            where,
            f"volume '{handle}' is a {context.types[handle]}, "
            f"a {wanted_type} is expected",
        )
    if not exclusive:
        return
    if handle in context.volume_users:
        checker.add(
            where,
            f"volume '{handle}' is already used by volume '{context.volume_users[handle]}'",
        )
    else:
        context.volume_users[handle] = user


def _raid_volume(
    checker: _Checker, params: dict, handle: str, where: str, context: _VolumeContext
) -> Optional[RaidParams]:
    data = checker.mapping(
        params,
        where,
        required=("partitions_handle", "level", "dev_indices", "raid_dev_name"),
        optional=("spare_indices", "meta_data"),
    )
    if data is None:
        return None
    layout_error = False
    partitions_handle = checker.string(
        data["partitions_handle"], f"{where}.partitions_handle"
    )
    level = checker.integer(data["level"], f"{where}.level")
    if level is not None and level not in RAID_LEVELS:
        checker.add(f"{where}.level", f"{level} is not one of {RAID_LEVELS}")
        level = None
    dev_indices = _indices(checker, data["dev_indices"], f"{where}.dev_indices")
    spare_indices = _indices(
        checker, data.get("spare_indices"), f"{where}.spare_indices"
    )
    meta_data = data.get("meta_data", "1.2")
    if isinstance(meta_data, bool) or not isinstance(meta_data, (str, int, float)):
        checker.add(f"{where}.meta_data", f"expected a version, got {meta_data!r}")
        meta_data = None
    elif str(meta_data) not in RAID_METADATA_VERSIONS:
        checker.add(
            f"{where}.meta_data",
            f"{meta_data!r} is not one of {RAID_METADATA_VERSIONS} "
            "(quote it in the yaml: 0.90 is read as 0.9)",
        )
        meta_data = None
    name = checker.string(data["raid_dev_name"], f"{where}.raid_dev_name")
    if name is not None:
        if not RAID_DEVICE_NAME_RE.match(name):
            checker.add(f"{where}.raid_dev_name", f"{name} is not of the form /dev/mdN")
        elif name in context.raid_names:
            checker.add(
                f"{where}.raid_dev_name",
                f"{name} is already used by volume '{context.raid_names[name]}'",
            )
        else:
            context.raid_names[name] = handle
    if partitions_handle is not None:
        _use_partitions(
            checker, context, [partitions_handle], handle, f"{where}.partitions_handle"
        )
        known = partitions_handle in context.partitions_per_handle
        if (
            known
            and level is not None
            and dev_indices is not None
            and spare_indices is not None
        ):
            try:
                check_raid_layout(
                    level,
                    dev_indices,
                    spare_indices,
                    context.partitions_per_handle[partitions_handle],
                    partitions_handle,
                )
            except ValueError as error:
                checker.add(where, str(error))
                layout_error = True
    if layout_error or None in (
        partitions_handle,
        level,
        dev_indices,
        spare_indices,
        meta_data,
        name,
    ):
        return None
    return RaidParams(
        partitions_handle=partitions_handle,
        level=level,
        dev_indices=dev_indices,
        raid_dev_name=name,
        spare_indices=spare_indices,
        meta_data=str(meta_data),
    )


def _indices(checker: _Checker, value: Any, where: str) -> Optional[List[int]]:
    if value is None:
        return []
    items = checker.sequence(value, where, allow_empty=True)
    if items is None:
        return None
    result = [checker.integer(item, f"{where}[{i}]") for i, item in enumerate(items)]
    return None if None in result else result


def _volume_params(
    checker: _Checker,
    volume_type: str,
    params: dict,
    handle: str,
    where: str,
    context: _VolumeContext,
) -> Optional[VolumeParams]:
    if volume_type == "raw":
        data = checker.mapping(params, where, required=("partitions_handle",))
        if data is None:
            return None
        partitions_handle = checker.string(
            data["partitions_handle"], f"{where}.partitions_handle"
        )
        if partitions_handle is None:
            return None
        _use_partitions(
            checker, context, [partitions_handle], handle, f"{where}.partitions_handle"
        )
        count = context.partitions_per_handle.get(partitions_handle)
        if count is not None and count != 1:
            checker.add(
                f"{where}.partitions_handle",
                f"a raw volume is one partition, '{partitions_handle}' has {count}",
            )
        return RawParams(partitions_handle)
    if volume_type == "raid":
        return _raid_volume(checker, params, handle, where, context)
    if volume_type == "volume_group":
        data = checker.mapping(
            params, where, optional=("partitions_handles", "volumes_handles")
        )
        if data is None:
            return None
        partitions = checker.strings(
            data.get("partitions_handles"), f"{where}.partitions_handles"
        )
        volumes = checker.strings(
            data.get("volumes_handles"), f"{where}.volumes_handles"
        )
        if not partitions and not volumes:
            checker.add(
                where, "a volume group needs partitions_handles or volumes_handles"
            )
        _use_partitions(
            checker, context, partitions, handle, f"{where}.partitions_handles"
        )
        for index, volume in enumerate(volumes):
            _use_volume(
                checker, context, volume, handle, f"{where}.volumes_handles[{index}]"
            )
        return VolumeGroupParams(partitions, volumes)
    # logical_volume
    data = checker.mapping(params, where, required=("volume_handle", "vg_percent"))
    if data is None:
        return None
    group = checker.string(data["volume_handle"], f"{where}.volume_handle")
    percent = data["vg_percent"]
    if isinstance(percent, bool) or not isinstance(percent, (int, float)):
        checker.add(f"{where}.vg_percent", f"expected a number, got {percent!r}")
        percent = None
    elif not 0 < percent <= 100:
        checker.add(
            f"{where}.vg_percent", f"must be above 0 and at most 100, got {percent}"
        )
        percent = None
    if group is None or percent is None:
        return None
    _use_volume(
        checker,
        context,
        group,
        handle,
        f"{where}.volume_handle",
        "volume_group",
        exclusive=False,
    )
    # logical volumes share the volume group, each one takes a part of it
    context.percent_per_group[group] += percent
    if context.percent_per_group[group] > 100 + 1e-9:
        checker.add(
            f"{where}.vg_percent",
            f"the logical volumes of '{group}' use "
            f"{context.percent_per_group[group]:g}% of it",
        )
    return LogicalVolumeParams(group, float(percent))


def _volumes(
    checker: _Checker, data: Any, partitions_per_handle: Dict[str, int]
) -> Tuple[List[VolumeSpec], _VolumeContext]:
    context = _VolumeContext(partitions_per_handle)
    volumes: List[VolumeSpec] = []
    if data is None:
        return volumes, context
    for index, item in enumerate(checker.sequence(data, "volumes", True) or []):
        where = f"volumes[{index}]"
        volume = checker.mapping(
            item, where, required=("handle", "type"), optional=("params",)
        )
        if volume is None:
            continue
        handle = checker.handle(volume["handle"], f"{where}.handle")
        volume_type = volume["type"]
        if volume_type not in VOLUME_TYPES:
            checker.add(
                f"{where}.type", f"{volume_type!r} is not one of {VOLUME_TYPES}"
            )
            continue
        if handle is None:
            continue
        if handle in context.types:
            checker.add(f"{where}.handle", f"volume handle {handle} is used twice")
            continue
        params = volume.get("params")
        params = {} if params is None else params
        spec = _volume_params(
            checker, volume_type, params, handle, f"{where}.params", context
        )
        # known from now on, even if its parameters are wrong, so that the
        # volumes using it do not report a second problem
        context.types[handle] = volume_type
        if spec is not None:
            volumes.append(VolumeSpec(handle, volume_type, spec))
    return volumes, context


# ----- file systems


def _file_systems(
    checker: _Checker, data: Any, context: _VolumeContext
) -> List[FileSystemSpec]:
    file_systems: List[FileSystemSpec] = []
    if data is None:
        return file_systems
    formatted_volumes: Dict[str, int] = {}
    mounts: Dict[str, int] = {}
    for index, item in enumerate(checker.sequence(data, "file_systems", True) or []):
        where = f"file_systems[{index}]"
        fs = checker.mapping(
            item,
            where,
            required=("type", "volume_handle"),
            optional=("format", "label", "mount", "mount_options", "fsck_pass"),
        )
        if fs is None:
            continue
        before = len(checker.errors)
        fs_type = fs["type"]
        if fs_type not in FSTAB_TYPES:
            checker.add(
                f"{where}.type", f"{fs_type!r} is not one of {tuple(FSTAB_TYPES)}"
            )
        volume = checker.string(fs["volume_handle"], f"{where}.volume_handle")
        if volume is not None:
            if volume not in context.types:
                checker.add(f"{where}.volume_handle", f"unknown volume '{volume}'")
            elif context.types[volume] not in FORMATTABLE_VOLUME_TYPES:
                checker.add(
                    f"{where}.volume_handle",
                    f"volume '{volume}' is a {context.types[volume]}, "
                    "it holds logical volumes, not a file system",
                )
            elif volume in context.volume_users:
                checker.add(
                    f"{where}.volume_handle",
                    f"volume '{volume}' is used by volume '{context.volume_users[volume]}', "
                    "it can not hold a file system",
                )
            elif volume in formatted_volumes:
                checker.add(
                    f"{where}.volume_handle",
                    f"volume '{volume}' already has the file system "
                    f"file_systems[{formatted_volumes[volume]}]",
                )
            else:
                formatted_volumes[volume] = index
        do_format = checker.boolean(fs.get("format"), f"{where}.format", default=True)
        label = fs.get("label")
        if label is not None:
            checker.string(label, f"{where}.label")
            if not do_format:
                checker.add(
                    f"{where}.label", "a volume that is not formatted keeps its label"
                )
        mount = fs.get("mount")
        if mount is not None:
            if fs_type == "swap":
                checker.add(f"{where}.mount", "swap has no mount point")
            elif (
                not isinstance(mount, str)
                or not MOUNT_RE.match(mount)
                or ".." in mount.split("/")
            ):
                checker.add(
                    f"{where}.mount",
                    f"must be an absolute path without spaces or '..', got {mount!r}",
                )
            else:
                mount = normalize_mount(mount)
                if mount in mounts:
                    checker.add(
                        f"{where}.mount",
                        f"{mount} is already used by file_systems[{mounts[mount]}]",
                    )
                mounts[mount] = index
        options = fs.get("mount_options")
        if options is not None and (
            not isinstance(options, str) or not OPTIONS_RE.match(options)
        ):
            checker.add(
                f"{where}.mount_options",
                f"expected options without spaces, got {options!r}",
            )
        fsck = fs.get("fsck_pass")
        if fsck is not None and checker.integer(fsck, f"{where}.fsck_pass") is not None:
            if fsck not in (0, 1, 2):
                checker.add(f"{where}.fsck_pass", f"must be 0, 1 or 2, got {fsck}")
        if len(checker.errors) == before:
            file_systems.append(
                FileSystemSpec(
                    type=fs_type,
                    volume_handle=volume,
                    format=do_format,
                    label=label,
                    mount=mount,
                    mount_options=options,
                    fsck_pass=fsck,
                )
            )
    return file_systems


# ----- entry points


def parse_recipe(data: Any) -> RecipeSpec:
    """Check a recipe already read from yaml.

    Raises:
        RecipeError: with every problem found
    """
    checker = _Checker()
    top = checker.mapping(
        data,
        "recipe",
        required=("partitioning",),
        optional=("volumes", "file_systems"),
    )
    if top is None:
        raise RecipeError(checker.errors)
    groups, partitions_per_handle = _partitioning(checker, top["partitioning"])
    volumes, context = _volumes(checker, top.get("volumes"), partitions_per_handle)
    file_systems = _file_systems(checker, top.get("file_systems"), context)
    if checker.errors:
        raise RecipeError(checker.errors)
    return RecipeSpec(groups, volumes, file_systems)


def loads_recipe(text: str) -> RecipeSpec:
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise RecipeError([f"recipe is not valid yaml: {error}"]) from error
    return parse_recipe(data)


def load_recipe(path: str) -> RecipeSpec:
    with open(path, "r", encoding="utf-8") as recipe_file:
        return loads_recipe(recipe_file.read())
