import copy
import os

import pytest
from device_guard import ensure_vg_absent
from device_helpers import ext_uuid, signatures

from samples import DISKS, RECIPE
from py_part_recipe.applier import Applier, ApplyError, apply_recipe
from py_part_recipe.fs import FormatError, supported
from py_part_recipe.recipe_loader import parse_recipe
from py_part_recipe.report import Report
from py_part_recipe.wipe import wipe_disk


def recipe(mutate=None):
    data = copy.deepcopy(RECIPE)
    if mutate:
        mutate(data)
    return parse_recipe(data)


@pytest.fixture
def disks():
    """Disks with nothing on them before and after, and the raid stopped."""
    ensure_vg_absent("vg_test")
    for disk in DISKS:
        wipe_disk(disk)
    yield DISKS
    for disk in DISKS:
        wipe_disk(disk)


# ----- applying a recipe


def test_apply_a_recipe(disks):
    # an old file system on a whole disk, the recipe wipes
    supported["ext4"](disks[0], "old").make_fs()
    assert signatures(disks[0]) == {"TYPE": "ext4"}

    report = apply_recipe(recipe())

    # what the recipe asked for exists
    assert signatures(disks[0]) == {"PTTYPE": "gpt"}
    assert os.path.exists("/dev/md100")
    assert os.path.exists("/dev/mapper/vg_test-lv_root")
    assert signatures("/dev/loop102p1")["TYPE"] == "ext2"
    assert signatures("/dev/loop102p2")["TYPE"] == "swap"
    assert signatures("/dev/vg_test/lv_root")["TYPE"] == "ext4"
    # and the report tells it
    assert sorted(report.partitions) == ["boot_part", "raid_part", "swap_part"]
    assert len(report.partitions["raid_part"]) == 2
    assert {h: (v.type, v.device) for h, v in report.volumes.items()} == {
        "md_vol": ("raid", "/dev/md100"),
        "vg_test": ("volume_group", "/dev/vg_test"),
        "lv_root": ("logical_volume", "/dev/vg_test/lv_root"),
        "lv_home": ("logical_volume", "/dev/vg_test/lv_home"),
        "boot_vol": ("raw", "/dev/loop102p1"),
        "swap_vol": ("raw", "/dev/loop102p2"),
    }
    by_volume = {fs.volume: fs for fs in report.file_systems}
    assert list(by_volume) == ["lv_root", "lv_home", "boot_vol", "swap_vol"]
    assert by_volume["lv_root"].label == "root"
    assert by_volume["lv_root"].uuid == ext_uuid("/dev/vg_test/lv_root")
    assert by_volume["lv_home"].mount_options == "noatime"
    assert by_volume["swap_vol"].mount is None
    assert [fs.mount for fs in report.mounted_file_systems()] == ["/", "/boot", "/home"]
    lines = report.fstab().splitlines()
    assert len(lines) == 4
    assert lines[0] == f"UUID={by_volume['lv_root'].uuid}\t/\text4\tdefaults\t0\t1"
    assert lines[-1] == f"UUID={by_volume['swap_vol'].uuid}\tnone\tswap\tsw\t0\t0"
    assert Report.loads(report.dumps("yaml")) == report


def test_apply_the_same_recipe_twice(disks):
    """A reinstall on the same disks: the first layout is still there, with the
    same partitions and so the same data area for the array, holding the lvm
    physical volume of the volume group."""
    first = apply_recipe(recipe())
    assert signatures("/dev/vg_test/lv_root")["TYPE"] == "ext4"

    second = apply_recipe(recipe())

    assert set(second.volumes) == set(first.volumes)
    assert signatures("/dev/vg_test/lv_root")["TYPE"] == "ext4"
    # a new file system, not the old one
    old = next(fs for fs in first.file_systems if fs.volume == "lv_root")
    new = next(fs for fs in second.file_systems if fs.volume == "lv_root")
    assert new.uuid != old.uuid


def test_volumes_of_disks_that_are_not_wiped_are_not_wiped():
    """Only what is built on the disks of a group that wipes is wiped."""

    def only_the_second_group_wipes(data):
        data["partitioning"][0]["recipe"]["wipe"] = False

    applier = Applier(recipe(only_the_second_group_wipes))
    assert applier._volumes_on_wiped_disks() == {"boot_vol", "swap_vol"}
    assert Applier(recipe())._volumes_on_wiped_disks() == {
        "md_vol",
        "vg_test",
        "lv_root",
        "lv_home",
        "boot_vol",
        "swap_vol",
    }


def test_a_volume_that_is_not_formatted_is_left_alone(disks):
    def keep_boot(data):
        data["file_systems"][2]["format"] = False

    report = apply_recipe(recipe(keep_boot))

    boot = next(fs for fs in report.file_systems if fs.volume == "boot_vol")
    assert (boot.formatted, boot.uuid, boot.label) == (False, None, None)
    assert signatures("/dev/loop102p1") == {}
    # nothing to name it by: the fstab leaves it out
    assert len(report.fstab().splitlines()) == 3


def test_the_stages_can_be_run_one_by_one(disks):
    applier = Applier(recipe())
    applier.partition()
    assert applier.partitions is not None and not applier.volumes.volumes
    assert signatures("/dev/loop102p1") == {}
    applier.build_volumes()
    assert os.path.exists("/dev/md100") and applier.formatted == []
    applier.format_file_systems()
    assert applier.formatted == ["lv_root", "lv_home", "boot_vol", "swap_vol"]
    assert applier.build_report() is applier.report


# ----- failures: the stage and what exists are told, nothing is undone


def test_failure_after_the_volumes_are_built(disks):
    def too_small_for_xfs(data):
        data["file_systems"][3].update(type="xfs")

    applier = Applier(recipe(too_small_for_xfs))
    with pytest.raises(ApplyError) as excinfo:
        applier.run()

    error = excinfo.value
    assert error.stage == "file_systems"
    assert isinstance(error.cause, FormatError)
    assert error.__cause__ is error.cause
    assert error.created["disks modified"] == DISKS
    assert error.created["volumes built"] == ["md_vol", "vg_test", "lv_root", "lv_home"]
    assert error.created["file systems formatted"] == ["lv_root", "lv_home", "boot_vol"]
    for text in ("file_systems", "Nothing was undone", "md_vol", "boot_vol"):
        assert text in str(error)
    # nothing was undone
    assert os.path.exists("/dev/md100")
    assert signatures("/dev/vg_test/lv_root")["TYPE"] == "ext4"


# these need no write: they fail before anything is changed


def test_not_enough_space_writes_nothing():
    def too_big(data):
        # more than any disk of the tests
        data["partitioning"][0]["recipe"]["partitions"][0]["min_size"] = "100 GiB"
        data["partitioning"][0]["recipe"]["partitions"][0]["max_size"] = "100 GiB"

    before = [signatures(disk) for disk in DISKS]
    with pytest.raises(ApplyError) as excinfo:
        apply_recipe(recipe(too_big))
    assert excinfo.value.stage == "partitioning"
    assert isinstance(excinfo.value.cause, ValueError)
    assert excinfo.value.created == {}
    assert [signatures(disk) for disk in DISKS] == before


def test_unknown_disk_writes_nothing():
    def unknown(data):
        data["partitioning"][1]["devices"] = ["/dev/loop107"]

    before = [signatures(disk) for disk in DISKS]
    with pytest.raises(ApplyError, match="loop107") as excinfo:
        apply_recipe(recipe(unknown))
    assert excinfo.value.stage == "partitioning"
    assert excinfo.value.created == {}
    # the first group is valid, but nothing was written for it either
    assert [signatures(disk) for disk in DISKS] == before


def test_volumes_need_partitions():
    applier = Applier(recipe())
    with pytest.raises(ApplyError) as excinfo:
        applier.build_volumes()
    assert excinfo.value.stage == "volumes"
    assert isinstance(excinfo.value.cause, RuntimeError)
    assert excinfo.value.created == {}


def test_apply_error_message():
    error = ApplyError(
        "volumes",
        LookupError("no such thing"),
        {"disks modified": ["/dev/sda"], "volumes built": [], "other": ["a", "b"]},
    )
    assert error.created == {"disks modified": ["/dev/sda"], "other": ["a", "b"]}
    assert str(error) == (
        "Applying the recipe failed at stage 'volumes': LookupError: no such thing. "
        "Nothing was undone. Disks modified: /dev/sda. Other: a, b."
    )
