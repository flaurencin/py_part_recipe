import copy

import parted
import pytest
import yaml

from py_part_recipe.recipe_loader import (
    LogicalVolumeParams,
    RaidParams,
    RawParams,
    RecipeError,
    VolumeGroupParams,
    load_recipe,
    loads_recipe,
    parse_recipe,
)

EXAMPLE = "examples/example_recipe.yml"
MIB = 1024 * 1024


def example() -> dict:
    with open(EXAMPLE, encoding="utf-8") as recipe_file:
        return yaml.safe_load(recipe_file)


def errors_of(data) -> list:
    with pytest.raises(RecipeError) as excinfo:
        parse_recipe(data)
    return excinfo.value.errors


# In the example:
#   partitioning[0]: loop100-102, partitions_raid_md0, _md1, _md2
#   partitioning[1]: loop103-105, partitions_raid_md4, _md5
#   partitioning[2]: loop106, partitions_boot, partitions_swap
#   volumes: 0 boot, 1 swap (raw), 2 md0, 3 md1, 4 md2, 5 md4, 6 md5 (raid),
#            7 vg_root, 8 vg_var (volume groups), 9 lv_root, 10 lv_home,
#            11 lv_var (logical volumes)
#   file_systems: 0 lv_root /, 1 lv_var /var, 2 lv_home /home, 3 boot /boot,
#                 4 md0 /boot/efi, 5 swap


def group(data, index=0):
    return data["partitioning"][index]


def partitions(data, index=0):
    return group(data, index)["recipe"]["partitions"]


def volume(data, index):
    return data["volumes"][index]["params"]


# ----- a valid recipe


def test_example_is_valid():
    recipe = load_recipe(EXAMPLE)
    assert len(recipe.partitioning) == 3
    assert len(recipe.volumes) == 12
    assert len(recipe.file_systems) == 6


def test_example_partitioning():
    first = load_recipe(EXAMPLE).partitioning[0]
    assert first.devices == ["/dev/loop100", "/dev/loop101", "/dev/loop102"]
    assert (first.keep_partitions, first.wipe) == (False, False)
    assert [part.handle for part in first.partitions] == [
        "partitions_raid_md0",
        "partitions_raid_md1",
        "partitions_raid_md2",
    ]
    md0 = first.partitions[0]
    assert (md0.min_size_bytes, md0.max_size_bytes) == (2 * MIB, 5 * MIB)
    assert md0.weight == 5000
    assert md0.p_type == parted.PARTITION_NORMAL
    assert md0.flags == [parted.PARTITION_RAID, getattr(parted, "PARTITION_ESP", 18)]


def test_example_volumes():
    volumes = {vol.handle: vol for vol in load_recipe(EXAMPLE).volumes}
    assert volumes["volume_boot"].params == RawParams("partitions_boot")
    assert volumes["volume_md1"].type == "raid"
    assert volumes["volume_md1"].params == RaidParams(
        partitions_handle="partitions_raid_md1",
        level=1,
        dev_indices=[0, 1],
        raid_dev_name="/dev/md31",
        spare_indices=[2],
        meta_data="1.0",
    )
    assert volumes["volume_vg_root"].params == VolumeGroupParams(
        [], ["volume_md1", "volume_md4"]
    )
    assert volumes["volume_lv_home"].params == LogicalVolumeParams(
        "volume_vg_root", 20.0
    )


def test_example_file_systems():
    file_systems = {fs.volume_handle: fs for fs in load_recipe(EXAMPLE).file_systems}
    root = file_systems["volume_lv_root"]
    assert (root.type, root.format, root.mount) == ("ext4", True, "/")
    assert file_systems["volume_lv_home"].mount_options == "noatime"
    swap = file_systems["volume_swap"]
    assert (swap.type, swap.mount) == ("swap", None)


def test_minimal_recipe_and_defaults():
    recipe = loads_recipe("""
partitioning:
  - devices: [/dev/loop100]
    recipe:
      partitions:
        - {handle: data, min_size: 1MiB, max_size: 2MiB, weight: 1}
""")
    (group_spec,) = recipe.partitioning
    assert (group_spec.keep_partitions, group_spec.wipe) == (False, False)
    (part,) = group_spec.partitions
    assert part.p_type == parted.PARTITION_NORMAL and part.flags == []
    assert recipe.volumes == [] and recipe.file_systems == []


def test_keys_left_empty_in_the_yaml_are_defaults():
    data = example()
    group(data)["recipe"]["keep_partitions"] = None
    group(data)["recipe"]["wipe"] = None
    data["volumes"][0]["params"] = {"partitions_handle": "partitions_boot"}
    data["file_systems"][0]["format"] = None
    recipe = parse_recipe(data)
    assert (recipe.partitioning[0].keep_partitions, recipe.partitioning[0].wipe) == (
        False,
        False,
    )
    assert recipe.file_systems[0].format is True


def test_sizes_can_be_numbers():
    data = example()
    partitions(data)[0].update(min_size=2 * MIB, max_size="5242880")
    first = parse_recipe(data).partitioning[0].partitions[0]
    assert (first.min_size_bytes, first.max_size_bytes) == (2 * MIB, 5 * MIB)


def test_mount_points_are_normalized():
    data = example()
    data["file_systems"][1]["mount"] = "//var/./"
    assert parse_recipe(data).file_systems[1].mount == "/var"


def test_a_logical_volume_group_can_hold_many_logical_volumes():
    data = example()
    assert data["volumes"][9]["params"]["volume_handle"] == "volume_vg_root"
    assert data["volumes"][10]["params"]["volume_handle"] == "volume_vg_root"
    parse_recipe(data)


def test_load_a_file_and_a_text(tmp_path):
    path = tmp_path / "recipe.yml"
    path.write_text(yaml.safe_dump(example()))
    assert load_recipe(str(path)) == loads_recipe(path.read_text())


def test_not_yaml():
    with pytest.raises(RecipeError, match="not valid yaml"):
        loads_recipe("partitioning: [unclosed")


@pytest.mark.parametrize("text", ("", "- a list", "3"))
def test_not_a_mapping(text):
    with pytest.raises(RecipeError, match="expected a mapping"):
        loads_recipe(text)


# ----- invalid recipes: break the example in one place, find the problem
# (mutation, text found in one of the errors)


def drop(mapping, key):
    del mapping[key]


def move_volume(data, source, target):
    data["volumes"].insert(target, data["volumes"].pop(source))


CASES = {
    # top level
    "unknown top level key": (lambda d: d.update(extra=1), "unknown key 'extra'"),
    "no partitioning": (
        lambda d: drop(d, "partitioning"),
        "missing key 'partitioning'",
    ),
    # devices
    "no device": (lambda d: group(d).update(devices=[]), "at least one device"),
    "device out of /dev": (
        lambda d: group(d)["devices"].__setitem__(0, "sda"),
        "is not under /dev/",
    ),
    "device twice in a group": (
        lambda d: group(d)["devices"].__setitem__(1, "/dev/loop100"),
        "/dev/loop100 is given twice",
    ),
    "device in two groups": (
        lambda d: group(d, 1)["devices"].__setitem__(0, "/dev/loop100"),
        "already used by partitioning[0]",
    ),
    # recipe
    "no partitions": (
        lambda d: drop(group(d)["recipe"], "partitions"),
        "missing key 'partitions'",
    ),
    "unknown recipe key": (
        lambda d: group(d)["recipe"].update(erase=True),
        "unknown key 'erase'",
    ),
    "wipe and keep": (
        lambda d: group(d)["recipe"].update(wipe=True, keep_partitions=True),
        "can not keep partitions",
    ),
    "keep is not a boolean": (
        lambda d: group(d)["recipe"].update(keep_partitions="yes"),
        "expected true or false",
    ),
    "empty partitions": (
        lambda d: group(d)["recipe"].update(partitions=[]),
        "expected a non empty list",
    ),
    # partitions
    "partition without handle": (
        lambda d: drop(partitions(d)[0], "handle"),
        "missing key 'handle'",
    ),
    "bad partition handle": (
        lambda d: partitions(d)[0].update(handle="bad handle!"),
        "Handle string must match",
    ),
    "partition handle used twice": (
        lambda d: partitions(d)[1].update(handle="partitions_raid_md0"),
        "handle partitions_raid_md0 is used twice",
    ),
    "partition handle used in two groups": (
        lambda d: partitions(d, 1)[0].update(handle="partitions_boot"),
        "handle partitions_boot is used twice",
    ),
    "unknown unit": (
        lambda d: partitions(d)[0].update(min_size="2 XB"),
        "unknown Unit",
    ),
    "min bigger than max": (
        lambda d: partitions(d)[0].update(min_size="10 GiB"),
        "min_size is bigger than max_size",
    ),
    "min is zero": (lambda d: partitions(d)[0].update(min_size=0), "more than 0"),
    "weight is zero": (lambda d: partitions(d)[0].update(weight=0), "at least 1"),
    "weight is not a number": (
        lambda d: partitions(d)[0].update(weight="heavy"),
        "expected an integer",
    ),
    "unknown flag": (
        lambda d: partitions(d)[0].update(flags=["nonexistent"]),
        "Invalid Partition Flag nonexistent",
    ),
    "flags is not a list": (
        lambda d: partitions(d)[0].update(flags="raid"),
        "expected a non empty list",
    ),
    "logical partition": (
        lambda d: partitions(d)[0].update(p_type="logical"),
        "Invalid Partition Type",
    ),
    # volumes
    "unknown volume type": (
        lambda d: d["volumes"][0].update(type="lvm"),
        "is not one of",
    ),
    "volume handle used twice": (
        lambda d: d["volumes"][1].update(handle="volume_boot"),
        "volume handle volume_boot is used twice",
    ),
    "raw on unknown partitions": (
        lambda d: volume(d, 0).update(partitions_handle="nope"),
        "unknown partitions handle 'nope'",
    ),
    "raw on partitions of several disks": (
        lambda d: volume(d, 0).update(partitions_handle="partitions_raid_md4"),
        "a raw volume is one partition, 'partitions_raid_md4' has 3",
    ),
    "partitions used by two volumes": (
        lambda d: volume(d, 1).update(partitions_handle="partitions_boot"),
        "already used by volume 'volume_boot'",
    ),
    "raid level": (
        lambda d: volume(d, 2).update(level=3),
        "is not one of (0, 1, 4, 5, 6, 10)",
    ),
    "raid 1 with three devices": (
        lambda d: volume(d, 2).update(dev_indices=[0, 1, 2], spare_indices=[]),
        "wrong number of devices for raid 1",
    ),
    "raid index out of range": (
        lambda d: volume(d, 2).update(dev_indices=[0, 5]),
        "Device index 5",
    ),
    "raid spare is a member": (
        lambda d: volume(d, 2).update(spare_indices=[1]),
        "common between raid and spare",
    ),
    "raid devices do not match the disks": (
        lambda d: volume(d, 2).update(spare_indices=[]),
        "You expect 2 devices including spares but 3 partitions",
    ),
    "raid index is not a number": (
        lambda d: volume(d, 2).update(dev_indices=[0, "one"]),
        "expected an integer",
    ),
    "raid device name": (
        lambda d: volume(d, 2).update(raid_dev_name="/dev/sda"),
        "not of the form /dev/mdN",
    ),
    "raid device name used twice": (
        lambda d: volume(d, 3).update(raid_dev_name="/dev/md30"),
        "already used by volume 'volume_md0'",
    ),
    "raid metadata": (lambda d: volume(d, 2).update(meta_data="2.0"), "is not one of"),
    "raid metadata read as a float": (
        lambda d: volume(d, 2).update(meta_data=0.9),
        "quote it in the yaml",
    ),
    "raid without level": (
        lambda d: drop(volume(d, 2), "level"),
        "missing key 'level'",
    ),
    "empty volume group": (
        lambda d: d["volumes"][7].update(params={}),
        "needs partitions_handles or volumes_handles",
    ),
    "volume group on an unknown volume": (
        lambda d: volume(d, 7).update(volumes_handles=["volume_nope"]),
        "unknown volume 'volume_nope'",
    ),
    "volume group defined before its volumes": (
        lambda d: move_volume(d, 7, 2),
        "must be defined before",
    ),
    "volume in two volume groups": (
        lambda d: volume(d, 8).update(volumes_handles=["volume_md1", "volume_md5"]),
        "volume 'volume_md1' is already used by volume 'volume_vg_root'",
    ),
    "logical volume on a raid": (
        lambda d: volume(d, 9).update(volume_handle="volume_md1"),
        "a volume_group is expected",
    ),
    "logical volume on an unknown group": (
        lambda d: volume(d, 9).update(volume_handle="volume_nope"),
        "unknown volume 'volume_nope'",
    ),
    "logical volumes too big for the group": (
        lambda d: volume(d, 10).update(vg_percent=50),
        "use 110% of it",
    ),
    "logical volume of nothing": (
        lambda d: volume(d, 9).update(vg_percent=0),
        "above 0 and at most 100",
    ),
    "logical volume size is not a number": (
        lambda d: volume(d, 9).update(vg_percent="half"),
        "expected a number",
    ),
    # file systems
    "unknown file system": (
        lambda d: d["file_systems"][0].update(type="zfs"),
        "is not one of",
    ),
    "file system on an unknown volume": (
        lambda d: d["file_systems"][0].update(volume_handle="nope"),
        "unknown volume 'nope'",
    ),
    "file system on a volume group": (
        lambda d: d["file_systems"][0].update(volume_handle="volume_vg_root"),
        "it holds logical volumes",
    ),
    "file system on a volume used by a group": (
        lambda d: d["file_systems"][3].update(volume_handle="volume_md1"),
        "used by volume 'volume_vg_root'",
    ),
    "two file systems on a volume": (
        lambda d: d["file_systems"][1].update(volume_handle="volume_lv_root"),
        "already has the file system file_systems[0]",
    ),
    "swap with a mount point": (
        lambda d: d["file_systems"][5].update(mount="/swap"),
        "swap has no mount point",
    ),
    "mount point used twice": (
        lambda d: d["file_systems"][1].update(mount="//"),
        "/ is already used by file_systems[0]",
    ),
    "relative mount point": (
        lambda d: d["file_systems"][1].update(mount="var"),
        "must be an absolute path",
    ),
    "parent directory in a mount point": (
        lambda d: d["file_systems"][1].update(mount="/a/../b"),
        "must be an absolute path",
    ),
    "fsck pass": (
        lambda d: d["file_systems"][0].update(fsck_pass=3),
        "must be 0, 1 or 2",
    ),
    "mount options with a space": (
        lambda d: d["file_systems"][0].update(mount_options="a b"),
        "options without spaces",
    ),
    "label of a volume that is not formatted": (
        lambda d: d["file_systems"][0].update(format=False, label="root"),
        "keeps its label",
    ),
    "install_mount of the previous format": (
        lambda d: d["file_systems"][0].update(install_mount="/"),
        "unknown key 'install_mount' (use 'mount'",
    ),
}


@pytest.mark.parametrize("mutate,expected", CASES.values(), ids=CASES.keys())
def test_invalid_recipe(mutate, expected):
    data = example()
    mutate(data)
    errors = errors_of(data)
    assert any(expected in error for error in errors), errors


def test_a_broken_volume_causes_one_error_only():
    """The volumes using it must not report it again as unknown."""
    data = example()
    volume(data, 3).update(level=3)  # volume_md1, used by volume_vg_root
    assert len(errors_of(data)) == 1


def test_all_the_problems_are_reported_at_once():
    data = example()
    partitions(data)[0].update(weight=0)
    volume(data, 2).update(level=3)
    data["file_systems"][0].update(type="zfs")
    errors = errors_of(data)
    assert len(errors) == 3
    assert errors[0].startswith("partitioning[0].recipe.partitions[0].weight")
    assert errors[1].startswith("volumes[2].params.level")
    assert errors[2].startswith("file_systems[0].type")


def test_errors_say_where_the_problem_is():
    data = example()
    partitions(data, 1)[1].update(max_size="1 XB")
    (error,) = errors_of(data)
    assert error.startswith("partitioning[1].recipe.partitions[1]: ")


def test_recipe_does_not_change_what_it_was_given():
    data = example()
    before = copy.deepcopy(data)
    parse_recipe(data)
    assert data == before
