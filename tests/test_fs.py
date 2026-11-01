import subprocess

import pytest

from py_part_recipe.fs import FormatError, supported

DEVICE = "/dev/loop106"


@pytest.fixture(autouse=True)
def blank_device():
    """Remove any signature so mkfs never asks for a confirmation."""
    with open(DEVICE, "r+b") as dev:
        dev.write(b"\0" * 1024 * 1024)
    yield


def read_signature(field: str) -> str:
    result = subprocess.run(
        ["blkid", "-p", "-s", field, "-o", "value", DEVICE],
        capture_output=True,
        check=True,
    )
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


def test_make_fs_failure_is_reported():
    ops = supported["ext4"](DEVICE, "data")
    ops.device = "/dev/does-not-exist"
    with pytest.raises(FormatError, match="mkfs.ext4"):
        ops.make_fs()
