import logging
from typing import Any, Literal
import pytest
import parted
import yaml
from py_part_recipe.partition_common import (
    BlockDevice,
    PartitionRequest,
    Partitionner,
    Recipe,
    convert_size_to_bytes,
)

LOGGER = logging.getLogger()


def setup_clean_disk(disk: str):
    disk = parted.Device(disk)
    ped_disk = parted.freshDisk(disk, "gpt")
    ped_disk.commitToDevice()
    try:
        ped_disk.commitToOS()
    except parted.IOException as excpt:
        LOGGER.warning(str(excpt))


def setup_partialy_used_disk(disk: str):
    disk = parted.Device(disk)
    ped_disk = parted.freshDisk(disk, "gpt")
    ped_disk.commitToDevice()
    geom1 = parted.Geometry(device=disk, start=2048, length=4)
    geom2 = parted.Geometry(device=disk, start=2052, length=4)
    partition1 = parted.Partition(
        disk=ped_disk, type=parted.PARTITION_NORMAL, geometry=geom1
    )
    partition2 = parted.Partition(
        disk=ped_disk, type=parted.PARTITION_NORMAL, geometry=geom2
    )
    ped_disk.addPartition(
        partition=partition1,
        constraint=parted.Constraint(exactGeom=geom1),
    )
    ped_disk.commitToDevice()
    partition1.setFlag(parted.PARTITION_RAID)
    ped_disk.commitToDevice()
    ped_disk.addPartition(
        partition=partition2,
        constraint=parted.Constraint(exactGeom=geom2),
    )
    partition2.setFlag(parted.PARTITION_RAID)
    ped_disk.commitToDevice()
    try:
        ped_disk.commitToOS()
    except parted.IOException as excpt:
        LOGGER.warning(str(excpt))


@pytest.mark.parametrize(
    "device,expected_nb_block,expected_block_size",
    (
        (
            "/dev/loop100",
            80_000 - 2_048 - 33,
            512,
        ),
    ),
)
def test_BlockDevice_on_clean_drive(
    device: Literal["/dev/loop100"],
    expected_nb_block: Literal[77919],
    expected_block_size: Literal[512],
):
    setup_clean_disk(device)
    loopx = BlockDevice(device)
    assert loopx.addressable_space.block_size == expected_block_size
    assert loopx.addressable_space.nb_block == expected_nb_block


@pytest.mark.parametrize(
    "device,expected_nb_block,expected_block_size",
    (
        (
            "/dev/loop100",
            80_000 - 2_056 - 33,
            512,
        ),
    ),
)
def test_BlockDevice_on_used_drive(
    device: Literal["/dev/loop100"],
    expected_nb_block: Literal[77911],
    expected_block_size: Literal[512],
):
    setup_partialy_used_disk(device)
    loopx = BlockDevice(device, keep_partitions=True)
    assert loopx.addressable_space.block_size == expected_block_size
    assert loopx.addressable_space.nb_block == expected_nb_block


@pytest.mark.parametrize(
    "size,expected_bytes",
    (
        ("5MB", 5_000_000),
        ("5kB", 5_000),
        ("5MiB", 5_242_880),
        ("5 MB", 5_000_000),
        ("5kB ", 5_000),
        (" 5MiB", 5_242_880),
    ),
)
def test_convert_size_to_bytes(
    size: Literal["5MB", "5kB", "5MiB", "5 MB", "5kB ", " 5MiB"],
    expected_bytes: Literal[5000000, 5000, 5242880],
):
    assert convert_size_to_bytes(size) == expected_bytes


@pytest.mark.parametrize(
    "size,expected_bytes",
    (
        ("M5MB", 5_000_000),
        ("5KB", 5_000),
        ("5iMiB", 5_242_880),
    ),
)
def test_raise_convert_size_to_bytes(
    size: Literal["M5MB", "5KB", "5iMiB"],
    expected_bytes: Literal[5000000, 5000, 5242880],
):
    with pytest.raises(ValueError):
        convert_size_to_bytes(size)


@pytest.mark.parametrize(
    "devices,partition_requests,expected_disk_dict",
    (
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    handle="patitions_1",
                    min_size="1MB",
                    max_size="2MiB",
                    weight=100,
                    p_type=parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
                PartitionRequest(
                    handle="patitions_2",
                    min_size="1MB",
                    max_size="2MiB",
                    weight=100,
                    p_type=parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
            ],
            0,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    handle="patitions_1",
                    min_size="1MiB",
                    max_size="100MiB",
                    weight=100,
                    p_type=parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
                PartitionRequest(
                    handle="patitions_2",
                    min_size="1MiB",
                    max_size="100MiB",
                    weight=100,
                    p_type=parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
            ],
            1,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    handle="patitions_1",
                    min_size="1MB",
                    max_size="2MiB",
                    weight=100,
                    p_type="normal",
                    flags=["raid"],
                ),
                PartitionRequest(
                    handle="patitions_2",
                    min_size="1MB",
                    max_size="2MiB",
                    weight=100,
                    p_type="normal",
                    flags=["raid"],
                ),
            ],
            0,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    handle="patitions_1",
                    min_size="1MiB",
                    max_size="100MiB",
                    weight=100,
                    p_type="normal",
                    flags=["raid"],
                ),
                PartitionRequest(
                    handle="patitions_2",
                    min_size="1MiB",
                    max_size="100MiB",
                    weight=100,
                    p_type="normal",
                    flags=["raid"],
                ),
            ],
            1,
        ),
    ),
)
def test_recipe_valid_same_partitions_keep_existing(
    devices: str,
    partition_requests: PartitionRequest,
    expected_disk_dict: dict[Any, Any],
):
    for device in devices:
        setup_partialy_used_disk(device)
    parting = Partitionner(Recipe(devices, partition_requests, keep_partitions=True))
    parting.create_partitions_mapping()
    expected_result_dict = yaml.safe_load(
        open("tests/test_recipe_valid.yaml", "r", encoding="utf-8"),
    )[expected_disk_dict]
    assert str(parting) == yaml.safe_dump(
        expected_result_dict,
        default_flow_style=False,
        sort_keys=False,
    )


@pytest.mark.parametrize(
    "devices,partition_requests,expected_disk_dict",
    (
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    "patitions_1",
                    "1MB",
                    "2MiB",
                    100,
                    parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
                PartitionRequest(
                    "patitions_2",
                    "1MB",
                    "2MiB",
                    100,
                    parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
            ],
            2,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    "patitions_1",
                    "1MiB",
                    "100MiB",
                    100,
                    parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
                PartitionRequest(
                    "patitions_2",
                    "1MiB",
                    "100MiB",
                    100,
                    parted.PARTITION_NORMAL,
                    flags=[parted.PARTITION_RAID],
                ),
            ],
            3,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    "patitions_1",
                    "1MB",
                    "2MiB",
                    100,
                    "normal",
                    flags=["raid"],
                ),
                PartitionRequest(
                    "patitions_2",
                    "1MB",
                    "2MiB",
                    100,
                    "normal",
                    flags=["raid"],
                ),
            ],
            2,
        ),
        (
            ["/dev/loop100", "/dev/loop101"],
            [
                PartitionRequest(
                    "patitions_1",
                    "10MiB",
                    "10MiB",
                    100,
                    "normal",
                    flags=["esp"],
                ),
                PartitionRequest(
                    "patitions_2",
                    "1MiB",
                    "100MiB",
                    100,
                    "normal",
                    flags=["raid"],
                ),
                PartitionRequest(
                    "patitions_3",
                    "1MiB",
                    "100MiB",
                    100,
                    "normal",
                    flags=["raid"],
                ),
            ],
            4,
        ),
    ),
)
def test_recipe_valid_same_partitions_flush_existing(
    devices: str,
    partition_requests: PartitionRequest,
    expected_disk_dict: dict[Any, Any],
):
    for device in devices:
        setup_partialy_used_disk(device)
    parting = Partitionner(Recipe(devices, partition_requests, keep_partitions=False))
    parting.create_partitions_mapping()
    expected_result_dict = yaml.safe_load(
        open("tests/test_recipe_valid.yaml", "r", encoding="utf-8"),
    )[expected_disk_dict]
    assert str(parting) == yaml.safe_dump(
        expected_result_dict,
        default_flow_style=False,
        sort_keys=False,
    )
