import os
import subprocess

import pytest

from py_part_recipe.fs import FormatError, supported

DEVICE = "/dev/loop106"
MIB = 1024 * 1024


def device_size() -> int:
    with open(DEVICE, "rb") as dev:
        return dev.seek(0, os.SEEK_END)


def require_device_size(min_mib: int):
    if device_size() < min_mib * MIB:
        pytest.fail(
            f"{DEVICE} is smaller than {min_mib} MiB, "
            "run pretest.sh again to grow it",
            pytrace=False,
        )


@pytest.fixture(autouse=True)
def blank_device():
    """Remove old signatures so a previous test can not interfere."""
    with open(DEVICE, "r+b") as dev:
        dev.write(b"\0" * MIB)
        # btrfs keeps a copy of its superblock at 64 MiB
        if device_size() > 65 * MIB:
            dev.seek(64 * MIB)
            dev.write(b"\0" * MIB)
    yield


def read_signature(field: str) -> str:
    result = subprocess.run(
        ["blkid", "-p", "-s", field, "-o", "value", DEVICE],
        capture_output=True,
        check=True,
    )
    return result.stdout.decode("utf-8").strip()


def read_label(fs_type: str) -> str:
    """Read a label with the tool of the file system when blkid is not exact.

    blkid reads at most 127 characters of an ntfs label, which can hold 128.
    """
    if fs_type != "ntfs":
        return read_signature("LABEL")
    result = subprocess.run(["ntfslabel", DEVICE], capture_output=True, check=True)
    return result.stdout.decode("utf-8").strip()


@pytest.mark.parametrize("fs_type", ("ext2", "ext3", "ext4"))
@pytest.mark.parametrize("label", ("data", "my data disk"))
def test_ext_make_fs(fs_type, label):
    supported[fs_type](DEVICE, label).make_fs()
    assert read_signature("TYPE") == fs_type
    assert read_signature("LABEL") == label


def test_ext_label_defaults_to_device_name():
    supported["ext4"](DEVICE, None).make_fs()
    assert read_signature("LABEL") == "loop106"


def test_ext_label_is_truncated():
    supported["ext4"](DEVICE, "a" * 20).make_fs()
    assert read_signature("LABEL") == "a" * 16


@pytest.mark.parametrize("fs_type,fat_bits", (("fat16", "16"), ("fat32", "32")))
def test_fat_make_fs(fs_type, fat_bits):
    supported[fs_type](DEVICE, "my boot").make_fs()
    assert read_signature("TYPE") == "vfat"
    assert read_signature("VERSION") == f"FAT{fat_bits}"
    # characters not allowed in a fat label are replaced by "_"
    assert read_signature("LABEL").upper() == "MY_BOOT"


# fs type, blkid TYPE, label with spaces that fits, minimum device size in MiB
OTHER_FS = (
    ("xfs", "xfs", "my xfs label", 300),
    ("btrfs", "btrfs", "my btrfs label", 120),
    ("exfat", "exfat", "my exfat la", 1),
    ("ntfs", "ntfs", "my ntfs label", 1),
    ("swap", "swap", "my swap", 1),
)


@pytest.mark.parametrize("fs_type,blkid_type,label,min_mib", OTHER_FS)
def test_other_make_fs(fs_type, blkid_type, label, min_mib):
    require_device_size(min_mib)
    supported[fs_type](DEVICE, label).make_fs()
    assert read_signature("TYPE") == blkid_type
    assert read_signature("LABEL") == label


@pytest.mark.parametrize("fs_type,blkid_type,label,min_mib", OTHER_FS)
def test_other_make_fs_over_existing_fs(fs_type, blkid_type, label, min_mib):
    """Formatting replaces what is already on the device."""
    require_device_size(min_mib)
    supported["ext4"](DEVICE, "old").make_fs()
    assert read_signature("TYPE") == "ext4"
    supported[fs_type](DEVICE, label).make_fs()
    assert read_signature("TYPE") == blkid_type


@pytest.mark.parametrize(
    "fs_type,limit,min_mib",
    (
        ("xfs", 12, 300),
        ("btrfs", 254, 120),
        ("exfat", 11, 1),
        ("ntfs", 128, 1),
        ("swap", 15, 1),
    ),
)
def test_other_label_is_truncated(fs_type, limit, min_mib):
    require_device_size(min_mib)
    supported[fs_type](DEVICE, "a" * (limit + 5)).make_fs()
    assert read_label(fs_type) == "a" * limit


def test_make_fs_failure_is_reported():
    ops = supported["ext4"](DEVICE, "data")
    ops.device = "/dev/does-not-exist"
    with pytest.raises(FormatError, match="mkfs.ext4"):
        ops.make_fs()


# Labels are computed when the object is created, nothing is formatted here.
@pytest.mark.parametrize(
    "fs_type,label,expected",
    (
        ("ext4", "a" * 20, "a" * 16),
        ("xfs", "a" * 20, "a" * 12),
        ("btrfs", "a" * 300, "a" * 254),
        ("exfat", "a" * 20, "a" * 11),
        ("ntfs", "a" * 200, "a" * 128),
        ("swap", "a" * 20, "a" * 15),
        ("fat32", "my boot disk!", "my_boot_dis"),
        # xfs counts bytes: 6 two bytes characters fit, a 7th is dropped whole
        ("xfs", "éàüéàüé", "éàüéàü"),
        # exfat counts characters
        ("exfat", "éàüéàüéàüéàü", "éàüéàüéàüéà"),
        # default: the device name
        ("ext4", None, "null"),
        ("xfs", "", "null"),
        ("swap", None, "null"),
    ),
)
def test_label(fs_type, label, expected):
    assert supported[fs_type]("/dev/null", label).label == expected


def test_unknown_device():
    with pytest.raises(ValueError, match="does not exist"):
        supported["xfs"]("/dev/does-not-exist", "data")


@pytest.mark.parametrize(
    "fs_type", ("ext4", "fat32", "xfs", "btrfs", "exfat", "ntfs", "swap")
)
def test_supported_types(fs_type):
    assert fs_type in supported
