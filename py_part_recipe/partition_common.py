from collections import defaultdict
import os
from dataclasses import dataclass, asdict, field
import json
from string import digits
from typing import Any, Dict, List, Union
import parted
from parted import Disk, DiskException
import yaml
from py_part_recipe.spacer import BlockChunk, ChunkableSpace, qualify_chunks
from logging import getLogger
from parted import __exponents, _ped

LOGGER = getLogger()
PARTITION_NAME_TO_P = {value: key for key, value in parted.partitions.items()}
if "esp" not in PARTITION_NAME_TO_P:
    PARTITION_NAME_TO_P.update(
        {
            "irst": 17,
            "esp": 18,
            "chrome_os_kernel": 19,
            "bls_boot": 20,
            "linux_home": 21,
            "no_auto_mount": 22,
        }
    )
PARTITION_P_TO_NAME = {value: key for key, value in PARTITION_NAME_TO_P.items()}


def partition_to_dict(part: parted.Partition):
    return {
        "active": part.active,
        "path": part.path,
        "number": part.number,
        "geometry": {
            "start": part.geometry.start,
            "end": part.geometry.end,
            "length": part.geometry.length,
        },
        "flags": part.getFlagsAsString(),
    }


def disk_to_dict(disk: Disk, with_partitions=True):
    partitions: List[parted.Partition] = list(disk.partitions)
    device: parted.Device = disk.device
    old_disk = parted.newDisk(disk.device)
    old_partitions: List[parted.Partition] = list(old_disk.partitions)
    disk.type
    data = {
        "type": disk.type,
        "model": device.model,
        "path": device.path,
        "physicalSectorSize": device.physicalSectorSize,
        "sectorSize": device.sectorSize,
        "length": device.length,
    }
    if with_partitions:
        data.update(
            {
                "partitions_before": [
                    partition_to_dict(part) for part in old_partitions
                ],
                "partitions_after": [partition_to_dict(part) for part in partitions],
            }
        )
    return data


def convert_size_to_bytes(size: str):
    """
    Converts Size expressed using Prefixes for binary multiples
    as defined by the NIST in the international system of units
    https://physics.nist.gov/cuu/Units/binary.html

    Using suffixes like
        "B":    1,       # byte
        "kB":   1000**1, # kilobyte
        "MB":   1000**2, # megabyte
        "GB":   1000**3, # gigabyte
        "TB":   1000**4, # terabyte
        "PB":   1000**5, # petabyte
        "EB":   1000**6, # exabyte
        "ZB":   1000**7, # zettabyte
        "YB":   1000**8, # yottabyte
        "KiB":  1024**1, # kibibyte
        "MiB":  1024**2, # mebibyte
        "GiB":  1024**3, # gibibyte
        "TiB":  1024**4, # tebibyte
        "PiB":  1024**5, # pebibyte
        "EiB":  1024**6, # exbibyte
        "ZiB":  1024**7, # zebibyte
        "YiB":  1024**8  # yobibyte
    """

    def index_first_char_not_in(string: str, excluded_chars: str):
        for index, char in enumerate(string):
            if char not in excluded_chars:
                return index

    size = str(size)
    if size.isnumeric():
        return int(size)
    size = size.strip()
    size = size.replace(" ", "")
    unit_position = index_first_char_not_in(size, digits)
    unit = size[unit_position:]
    if unit not in __exponents:
        raise ValueError(
            f"unknown Unit {unit} unit must be in {' ,'.join(__exponents.keys())}"
        )
    quantity_str = size[:unit_position]
    if not quantity_str.isnumeric():
        raise (f"Size ({size}) quantity of Unit {unit} does not seem to be a number")
    quantity = int(quantity_str)
    return quantity * __exponents[unit]


@dataclass
class BlockDevice:
    path: str
    dev_name: str = field(init=False)
    dev_sys_path: str = field(init=False)
    addressable_space: ChunkableSpace = field(init=False)
    keep_partitions: bool = False
    # base offset is in blocks general agreement
    # is to not use 2048 first blocks of the drive
    base_offset: int = 2048
    # footer protects gpt footer
    footer: int = 33

    def to_dict(self):
        return asdict(self)

    def to_json(self, pretty=False):
        if pretty:
            return json.dumps(self.to_dict(), indent=4)
        return json.dumps(self.to_dict())

    def __post_init__(self):
        self._set_device_info()
        self._set_adressable_space()

    def _set_device_info(self):
        device = self.path
        if not device.startswith("/dev/"):
            raise ValueError("device path must start with /dev/")
        if not os.path.exists(device):
            raise LookupError(f"Device {device} not found")
        if os.path.islink(device):
            device = os.readlink(device)
        device_name = device.strip("/").split("/")[-1]
        if device_name not in os.listdir("/sys/class/block"):
            raise ValueError(f"Device {device} is not a block device")
        self.dev_name = device_name
        self.dev_sys_path = os.readlink(f"/sys/class/block/{device_name}")
        phy_dev_position = self.dev_sys_path.split("/").index("block") + 1
        partitionable_dev_name = self.dev_sys_path.split("/")[phy_dev_position]
        physical_device = f"/dev/{partitionable_dev_name}"
        if physical_device != self.path:
            raise ValueError(
                f"Selected device {self.path} is not a patitionable block device"
            )

    def _set_adressable_space(self):
        disk = parted.Device(self.path)
        try:
            parted_disk = parted.newDisk(disk)
        except DiskException:
            parted_disk = None

        if self.keep_partitions and parted_disk:
            geometries = parted_disk.getFreeSpaceRegions()
            geometries.sort(key=lambda g: g.end - g.start + 1)
            largest_free = geometries[-1]
            nb_block = largest_free.end - largest_free.start + 1
            # set the offset where the larget free block starts
            self.base_offset = largest_free.start
            self.addressable_space = ChunkableSpace(nb_block, disk.physicalSectorSize)
        else:
            adrressable = disk.length - self.base_offset - self.footer
            self.addressable_space = ChunkableSpace(
                adrressable, disk.physicalSectorSize
            )


@dataclass
class PartitionRequest:
    handle: str
    min_size: str
    min_size_bytes: int = field(init=False)
    max_size: str
    max_size_bytes: int = field(init=False)
    weight: int
    # defaults to parted.PARTITION_NORMAL
    p_type: Union[str, int] = parted.PARTITION_NORMAL
    flags: List[Union[str, int]] = field(default_factory=list)

    def __post_init__(self):
        self.min_size_bytes = convert_size_to_bytes(self.min_size)
        self.max_size_bytes = convert_size_to_bytes(self.max_size)
        if isinstance(self.p_type, str):
            self.p_type = PARTITION_NAME_TO_P[self.p_type]
        VALID_TYPES = [
            parted.PARTITION_NORMAL,
            parted.PARTITION_LOGICAL,
            parted.PARTITION_EXTENDED,
            parted.PARTITION_FREESPACE,
            parted.PARTITION_METADATA,
            parted.PARTITION_PROTECTED,
        ]
        if self.p_type not in VALID_TYPES:
            raise ValueError(f"Invalid Partition Type {self.p_type}")
        for index, flag in enumerate(self.flags):
            if isinstance(flag, str):
                flag = PARTITION_NAME_TO_P[flag]
                self.flags[index] = flag
            if flag not in PARTITION_P_TO_NAME:
                raise ValueError(f"Invqlid Pqrtition Flag {flag}")


@dataclass
class Recipe:
    devices_names: List[str]
    part_requests: List[PartitionRequest] = field(default_factory=list)
    devices: List[BlockDevice] = field(init=False)
    block_chunks: List[BlockChunk] = field(init=False)
    common_partition_type: Union[None, str] = field(init=False, default=None)
    common_space: int = field(init=False, default=0)
    keep_partitions: bool = False

    def __post_init__(self):
        self.devices = [
            BlockDevice(dev_name, self.keep_partitions)
            for dev_name in self.devices_names
        ]
        self._set_commons()

    def _set_commons(self):
        """
        set common elements for devices:
            - block size
            - space
            - partition table type
        """
        self.common_space = min(
            [
                dev.addressable_space.nb_block * dev.addressable_space.block_size
                for dev in self.devices
            ]
        )
        self.common_block_size = max(
            [dev.addressable_space.block_size for dev in self.devices]
        )
        self.common_space = (
            self.common_space // self.common_block_size * self.common_block_size
        )
        disks_table_types = [parted.Device(device.path).type for device in self.devices]
        error = ", ".join(
            [
                f"{disk.path}: {ttype}"
                for ttype, disk in zip(disks_table_types, self.devices)
            ]
        )
        if self.keep_partitions and len(set(disks_table_types)) > 1:
            raise ValueError(
                f"All disks do not share the same partitionning tabe type {error}"
            )


class Partitionner:
    valid_ptable_type = ("gpt", "msdos")

    def __init__(self, recipe: Recipe, table_type: str = "gpt"):
        self.recipe = recipe
        self.disks: List[parted.Disk] = []
        self.created_partitions_per_dev: Dict[
            str, List[parted.Partition]
        ] = defaultdict(list)
        self.created_parttions_by_handle: Dict[
            str, List[parted.Partition]
        ] = defaultdict(list)

        if table_type in self.valid_ptable_type:
            self.ptable_type = table_type
        else:
            raise ValueError(
                "this is meant to be used for Linux usage and only support gpt and msdos "
                "partiotionning table."
            )

    def _figure_out_partitions(self):
        block_chunks = [
            BlockChunk(
                part_req.min_size_bytes, part_req.max_size_bytes, part_req.weight
            )
            for part_req in self.recipe.part_requests
        ]
        space = ChunkableSpace(
            self.recipe.common_space // self.recipe.common_block_size,
            self.recipe.common_block_size,
        )
        self.block_chunks = qualify_chunks(space, block_chunks)

    def create_partitions_mapping(self):
        if self.recipe.keep_partitions:
            disks = [
                parted.newDisk(parted.Device(disk.path)) for disk in self.recipe.devices
            ]
        else:
            disks = [
                parted.freshDisk(parted.Device(disk.path), self.ptable_type)
                for disk in self.recipe.devices
            ]
        self.disks = disks
        self._figure_out_partitions()
        for disk_index, device in enumerate(self.recipe.devices):
            disk = self.disks[disk_index]
            offset = device.base_offset
            for part_index, chunk in enumerate(self.block_chunks):
                part_req = self.recipe.part_requests[part_index]
                partition_type = part_req.p_type
                qty_sectors = chunk.optimal_final_size // self.recipe.common_block_size
                geom = parted.Geometry(
                    device=disk.device, start=offset, length=qty_sectors
                )
                offset += qty_sectors
                partition = parted.Partition(
                    disk=disk, type=partition_type, geometry=geom
                )
                self.created_partitions_per_dev[device.path].append(partition)
                self.created_parttions_by_handle[part_req.handle].append(partition)
                disk.addPartition(
                    partition=partition,
                    constraint=parted.Constraint(exactGeom=geom),
                )
                for flag in part_req.flags:
                    partition.setFlag(flag)

    def commit_to_devices(self):
        for disk in self.disks:
            disk.commitToDevice()

    def commit_to_os(self):
        for disk in self.disks:
            disk.commitToOS()

    def commit(self):
        for disk in self.disks:
            disk.commit()

    def _serializable_mapping(self, by_handle=False):
        if by_handle:
            data = {
                handle: [partition_to_dict(part for part in parts)]
                for handle, parts in self.created_parttions_by_handle.items()
            }
            return data
        data = [disk_to_dict(disk) for disk in self.disks]
        return data

    def __repr__(self):
        return yaml.safe_dump(
            self._serializable_mapping(), default_flow_style=False, sort_keys=False
        )

    def __str__(self):
        return self.__repr__()


class MultiPartitionner:
    def __init__(self, partitionners: List[Partitionner]) -> None:
        self.partitionners = partitionners
        self.created = False
        self.saved = False
        self.committed_to_os = False

    def create_partitions_mapping(self):
        for part in self.partitionners:
            part.create_partitions_mapping()

    def commit_to_devices(self):
        for part in self.partitionners:
            part.commit_to_devices()
        self.saved = True

    def commit_to_os(self):
        for part in self.partitionners:
            part.commit_to_os()
        self.committed_to_os = True

    def commit(self):
        for part in self.partitionners:
            part.commit()
        self.saved = True
        self.committed_to_os = True

    def get_partitions_by_handle(self, handle: str) -> List[parted.partitions]:
        partitions: List[parted.partitions] = []
        for partitionner in self.partitionners:
            if handle in partitionner.created_parttions_by_handle:
                partitions.extend(partitionner.created_parttions_by_handle[handle])
        return partitions

    def __repr__(self):
        data = {}
        [
            data.update(partitionner._serializable_mapping(by_handle=True))
            for partitionner in self.partitionners
        ]
        return yaml.safe_dump(data)

    def __str__(self):
        return self.__repr__()
