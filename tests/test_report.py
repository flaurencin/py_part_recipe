import copy
import json
import logging

import pytest
import yaml

from py_part_recipe.report import (
    FileSystemReport,
    PartitionReport,
    Report,
    ReportError,
    VolumeReport,
    default_fsck_pass,
)


def sample_report() -> Report:
    return Report(
        partitions={
            "partitions_boot": [
                PartitionReport("/dev/loop106p1", "/dev/loop106", 1, "aaaa-1", 52428800)
            ],
            "partitions_raid_md1": [
                PartitionReport(
                    "/dev/loop100p2", "/dev/loop100", 2, "bbbb-2", 104857600
                ),
                PartitionReport(
                    "/dev/loop101p2", "/dev/loop101", 2, "cccc-3", 104857600
                ),
            ],
        },
        volumes={
            "volume_boot": VolumeReport("raw", "/dev/loop106p1", None),
            "volume_swap": VolumeReport("raw", "/dev/loop106p2", None),
            "volume_md1": VolumeReport("raid", "/dev/md31", "md-uuid-1"),
            "volume_lv_root": VolumeReport(
                "logical_volume", "/dev/vg/lv_root", "lv-uuid-1"
            ),
            "volume_lv_var": VolumeReport(
                "logical_volume", "/dev/vg/lv_var", "lv-uuid-2"
            ),
        },
        # not in mount order on purpose
        file_systems=[
            FileSystemReport(
                "volume_lv_var",
                "ext4",
                True,
                "/dev/vg/lv_var",
                "uuid-var",
                "var",
                "/var",
            ),
            FileSystemReport(
                "volume_boot",
                "fat32",
                True,
                "/dev/loop106p1",
                "ABCD-1234",
                mount="/boot/efi",
            ),
            FileSystemReport(
                "volume_swap", "swap", True, "/dev/loop106p2", "uuid-swap"
            ),
            FileSystemReport(
                "volume_lv_root",
                "ext4",
                True,
                "/dev/vg/lv_root",
                "uuid-root",
                "root",
                "/",
            ),
            FileSystemReport(
                "volume_md1", "xfs", False, "/dev/md31", "uuid-md1", mount="/data"
            ),
        ],
    )


@pytest.mark.parametrize("fmt", ("yaml", "json"))
def test_round_trip(fmt):
    report = sample_report()
    assert Report.loads(report.dumps(fmt)) == report


def test_json_is_yaml():
    assert yaml.safe_load(sample_report().dumps("json")) == sample_report().to_dict()


def test_dumps_unknown_format():
    with pytest.raises(ValueError, match="yaml or json"):
        sample_report().dumps("xml")


@pytest.mark.parametrize("name", ("report.yml", "report.json"))
def test_dump_and_load_file(tmp_path, name):
    path = tmp_path / name
    sample_report().dump(str(path))
    assert Report.load(str(path)) == sample_report()
    is_json = name.endswith(".json")
    assert (path.read_text().lstrip()[0] == "{") == is_json


def test_defaults_are_explicit():
    ext_root = FileSystemReport("v", "ext4", True, "/dev/x", "u", mount="/")
    assert (ext_root.mount_options, ext_root.fsck_pass) == ("defaults", 1)
    swap = FileSystemReport("v", "swap", True, "/dev/x", "u")
    assert (swap.mount_options, swap.fsck_pass) == ("sw", 0)


@pytest.mark.parametrize(
    "fs_type,mount,expected",
    (
        ("ext4", "/", 1),
        ("ext4", "/var", 2),
        ("ext2", "/boot", 2),
        ("fat32", "/boot/efi", 2),
        ("xfs", "/", 0),
        ("btrfs", "/", 0),
        ("ntfs", "/win", 0),
        ("exfat", "/usb", 0),
        ("swap", None, 0),
        ("ext4", None, 0),
    ),
)
def test_default_fsck_pass(fs_type, mount, expected):
    assert default_fsck_pass(fs_type, mount) == expected


def test_mount_order_is_parent_first():
    mounts = [fs.mount for fs in sample_report().mounted_file_systems()]
    assert mounts == ["/", "/var", "/data", "/boot/efi"]
    for index, mount in enumerate(mounts):
        for parent in mounts[:index]:
            assert not parent.startswith(mount + "/")


def test_fstab():
    assert sample_report().fstab() == (
        "UUID=uuid-root\t/\text4\tdefaults\t0\t1\n"
        "UUID=uuid-var\t/var\text4\tdefaults\t0\t2\n"
        "UUID=uuid-md1\t/data\txfs\tdefaults\t0\t0\n"
        "UUID=ABCD-1234\t/boot/efi\tvfat\tdefaults\t0\t2\n"
        "UUID=uuid-swap\tnone\tswap\tsw\t0\t0\n"
    )


def test_fstab_leaves_out_file_systems_without_uuid(caplog):
    report = sample_report()
    report.file_systems[0].uuid = None
    with caplog.at_level(logging.WARNING):
        fstab = report.fstab()
    assert "/var" not in fstab
    assert "volume_lv_var" in caplog.text


def test_fstab_of_a_report_without_mount_is_empty():
    assert Report().fstab() == ""


def test_fstab_options_and_pass_come_from_the_report():
    report = sample_report()
    root = report.file_systems[3]
    root.mount_options = "noatime,errors=remount-ro"
    root.fsck_pass = 2
    assert (
        "UUID=uuid-root\t/\text4\tnoatime,errors=remount-ro\t0\t2\n" in report.fstab()
    )


# ----- invalid reports


def errors_of(data) -> list:
    with pytest.raises(ReportError) as excinfo:
        Report.from_dict(data)
    return excinfo.value.errors


def valid_dict() -> dict:
    return copy.deepcopy(sample_report().to_dict())


def test_valid_dict():
    assert Report.from_dict(valid_dict()) == sample_report()


@pytest.mark.parametrize("data", (None, [], "text", 3))
def test_not_a_mapping(data):
    assert "expected a mapping" in errors_of(data)[0]


def test_not_yaml():
    with pytest.raises(ReportError, match="not valid yaml"):
        Report.loads("a: [unclosed")


def test_unknown_and_missing_top_level_keys():
    data = valid_dict()
    del data["volumes"]
    data["extra"] = 1
    assert errors_of(data) == [
        "report: unknown key 'extra'",
        "report: missing key 'volumes'",
    ]


def test_unsupported_version():
    data = valid_dict()
    data["version"] = 2
    assert "version 2 is not supported" in errors_of(data)[0]


def test_version_type():
    data = valid_dict()
    data["version"] = "1"
    assert errors_of(data) == ["version: expected int, got str"]


def test_unknown_and_missing_keys_in_items():
    data = valid_dict()
    data["file_systems"][0]["colour"] = "red"
    del data["file_systems"][1]["uuid"]
    del data["volumes"]["volume_md1"]["device"]
    data["partitions"]["partitions_boot"][0]["size"] = 1
    del data["partitions"]["partitions_boot"][0]["size_bytes"]
    assert sorted(errors_of(data)) == sorted(
        [
            "file_systems[0]: unknown key 'colour'",
            "file_systems[1]: missing key 'uuid'",
            "volumes.volume_md1: missing key 'device'",
            "partitions.partitions_boot[0]: unknown key 'size'",
            "partitions.partitions_boot[0]: missing key 'size_bytes'",
        ]
    )


def test_wrong_types():
    data = valid_dict()
    data["file_systems"][0]["formatted"] = "yes"
    data["partitions"]["partitions_boot"][0]["number"] = True
    data["volumes"]["volume_md1"]["uuid"] = 12
    assert sorted(errors_of(data)) == sorted(
        [
            "file_systems[0].formatted: expected bool, got str",
            "partitions.partitions_boot[0].number: unexpected boolean",
            "volumes.volume_md1.uuid: expected str/NoneType, got int",
        ]
    )


def test_all_errors_are_reported_at_once():
    data = valid_dict()
    data["file_systems"][0]["volume"] = "volume_nowhere"
    data["file_systems"][1]["type"] = "zfs"
    data["file_systems"][2]["mount"] = "/swap"
    assert len(errors_of(data)) == 3


def test_unknown_volume_and_type():
    data = valid_dict()
    data["file_systems"][0]["volume"] = "volume_nowhere"
    data["file_systems"][1]["type"] = "zfs"
    errors = errors_of(data)
    assert any("unknown volume 'volume_nowhere'" in error for error in errors)
    assert any("unknown file system type 'zfs'" in error for error in errors)


@pytest.mark.parametrize("mount", ("var", "/va r", "/a/../b", "", "swap"))
def test_invalid_mount(mount):
    data = valid_dict()
    data["file_systems"][0]["mount"] = mount
    assert "mount must be an absolute path" in errors_of(data)[0]


def test_swap_with_a_mount_point():
    data = valid_dict()
    data["file_systems"][2]["mount"] = "/swap"
    assert "swap has no mount point" in errors_of(data)[0]


@pytest.mark.parametrize("duplicate", ("/", "//", "/var/", "/var/."))
def test_duplicate_mount(duplicate):
    data = valid_dict()
    data["file_systems"][4]["mount"] = duplicate
    errors = errors_of(data)
    assert any("already used by" in error for error in errors)


@pytest.mark.parametrize(
    "mount,expected",
    (("/", "/"), ("//", "/"), ("/var/", "/var"), ("//var/./log//", "/var/log")),
)
def test_mount_is_normalized(mount, expected):
    assert (
        FileSystemReport("v", "ext4", True, "/dev/x", "u", mount=mount).mount
        == expected
    )


def test_invalid_fsck_pass():
    data = valid_dict()
    data["file_systems"][0]["fsck_pass"] = 3
    assert "fsck_pass must be 0, 1 or 2" in errors_of(data)[0]


def test_loading_an_invalid_file_names_the_problem(tmp_path):
    data = valid_dict()
    data["file_systems"][0]["volume"] = "volume_nowhere"
    path = tmp_path / "report.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ReportError, match="volume_nowhere"):
        Report.load(str(path))
