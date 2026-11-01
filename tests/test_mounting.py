import os
import subprocess

import pytest
from samples import sample_report

from py_part_recipe.mounting import (
    BIND_SOURCES,
    MountError,
    UmountError,
    current_mounts,
    mount_all,
    parse_mountinfo,
    plan_mounts,
    umount_all,
    umount_targets,
)

BIND_TARGETS = ["dev", "dev/pts", "proc", "sys", "run"]


def mounted_points(under: str) -> list[str]:
    under = os.path.realpath(under)
    return [
        mount.mount_point
        for mount in current_mounts()
        if mount.mount_point == under or mount.mount_point.startswith(under + "/")
    ]


# ----- reading /proc/self/mountinfo


def test_parse_mountinfo():
    text = (
        "36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue\n"
        "37 36 0:3 / /with\\040space\\134backslash rw shared:1 shared:2 - tmpfs tmpfs rw\n"
        "38 36 0:4 / /bare rw - proc proc rw\n"
        "garbage line\n"
        "\n"
    )
    first, second, third = parse_mountinfo(text)
    assert (first.mount_id, first.mount_point, first.fs_type) == (36, "/mnt2", "ext3")
    assert (first.source, first.options) == ("/dev/root", "rw,noatime")
    assert second.mount_point == "/with space\\backslash"
    assert (second.fs_type, second.source) == ("tmpfs", "tmpfs")
    assert third.mount_point == "/bare"


def test_the_mounts_of_this_system():
    mounts = {mount.mount_point: mount for mount in current_mounts()}
    assert mounts["/proc"].fs_type == "proc"
    assert "/" in mounts


# ----- the plan, nothing is mounted


def test_plan_mounts_parent_first(tmp_path):
    root = str(tmp_path / "target")
    steps = plan_mounts(sample_report(), root)
    assert [step.target for step in steps] == [
        root,
        f"{root}/var",
        f"{root}/data",
        f"{root}/boot/efi",
    ]
    assert steps[0].command == [
        "mount",
        "-t",
        "ext4",
        "-o",
        "defaults",
        "UUID=uuid-root",
        root,
    ]
    # by uuid, with the type fstab uses, and the options of the report
    assert steps[2].command[:4] == ["mount", "-t", "xfs", "-o"]
    assert steps[3].command[2] == "vfat"
    assert steps[3].command[5] == "UUID=ABCD-1234"


def test_plan_does_not_mount_swap(tmp_path):
    steps = plan_mounts(sample_report(), str(tmp_path))
    assert all("swap" not in step.command for step in steps)
    assert all("UUID=uuid-swap" not in step.command for step in steps)


def test_plan_binds_the_system_after_the_file_systems(tmp_path):
    root = str(tmp_path / "target")
    steps = plan_mounts(sample_report(), root, bind_system=True)
    binds = steps[-5:]
    assert [step.target for step in binds] == [f"{root}/{rel}" for rel in BIND_TARGETS]
    assert [step.command[:3] for step in binds] == [
        ["mount", "--bind", source] for source, _ in BIND_SOURCES
    ]
    assert len(steps) == 4 + 5
    assert len(plan_mounts(sample_report(), root)) == 4


def test_plan_refuses_the_root_of_the_system(tmp_path):
    for root in ("/", "//", "/."):
        with pytest.raises(MountError, match="Refusing to use /"):
            plan_mounts(sample_report(), root)
    link = tmp_path / "link"
    link.symlink_to("/")
    with pytest.raises(MountError, match="Refusing to use /"):
        plan_mounts(sample_report(), str(link))


def test_plan_refuses_a_file_system_without_uuid(tmp_path):
    """Skipping it could mount its children over the host's directories."""
    report = sample_report()
    report.file_systems[3].uuid = None  # the root
    with pytest.raises(
        MountError, match="volume_lv_root.*no UUID.*Nothing was mounted"
    ):
        plan_mounts(report, str(tmp_path))


def test_mount_all_refuses_a_root_that_is_mounted():
    # nothing is run: these are real mount points of this system
    for root in ("/proc", "/dev"):
        with pytest.raises(MountError, match="already mounted, nothing was mounted"):
            mount_all(sample_report(), root)


def test_mount_all_refuses_a_root_of_the_system():
    with pytest.raises(MountError, match="Refusing to use /"):
        mount_all(sample_report(), "/")


# ----- unmounting what is not mounted


def test_umount_order_is_the_reverse_of_the_mount_order(tmp_path):
    root = str(tmp_path / "target")
    targets = umount_targets(sample_report(), root)
    mounted = [step.target for step in plan_mounts(sample_report(), root, True)]
    assert targets == list(reversed(mounted))
    assert targets[:5] == [f"{root}/{rel}" for rel in reversed(BIND_TARGETS)]


def test_umount_what_is_not_mounted_does_nothing(tmp_path):
    root = str(tmp_path / "target")
    result = umount_all(sample_report(), root)
    assert result.unmounted == []
    assert len(result.not_mounted) == 4 + 5
    # twice is the same
    assert umount_all(sample_report(), root) == result


def test_umount_refuses_the_root_of_the_system():
    with pytest.raises(MountError, match="Refusing to use /"):
        umount_all(sample_report(), "/")


# ----- on the loop devices


def mount_by_target(root):
    return {
        mount.mount_point: mount
        for mount in current_mounts()
        if mount.mount_point.startswith(os.path.realpath(root))
    }


def test_mount_then_umount(applied):
    report, root = applied
    real = os.path.realpath(root)

    mounted = mount_all(report, root)

    assert mounted == [root, f"{root}/boot", f"{root}/home"]
    mounts = mount_by_target(root)
    # by uuid: the right device is mounted at the right place
    assert (mounts[real].fs_type, mounts[real].source) == (
        "ext4",
        "/dev/mapper/vg_test-lv_root",
    )
    assert (mounts[f"{real}/boot"].fs_type, mounts[f"{real}/boot"].source) == (
        "ext2",
        "/dev/loop102p1",
    )
    assert mounts[f"{real}/home"].source == "/dev/mapper/vg_test-lv_home"
    assert "noatime" in mounts[f"{real}/home"].options
    # the parent was mounted before its children
    assert (
        mounts[real].mount_id
        < mounts[f"{real}/boot"].mount_id
        < mounts[f"{real}/home"].mount_id
    )
    # and nothing else, swap is not activated
    assert len(mounts) == 3

    result = umount_all(report, root)

    assert result.unmounted == [f"{root}/home", f"{root}/boot", root]
    assert mounted_points(root) == []
    again = umount_all(report, root)
    assert again.unmounted == [] and len(again.not_mounted) == 3 + 5


def test_mount_twice(applied):
    report, root = applied
    mount_all(report, root)
    with pytest.raises(MountError, match="already mounted, nothing was mounted"):
        mount_all(report, root)
    assert len(mount_by_target(root)) == 3


def test_a_failed_mount_stops_and_unmounts_nothing(applied):
    report, root = applied
    for fs in report.file_systems:
        if fs.volume == "lv_home":
            fs.mount_options = "nosuchoption"

    with pytest.raises(MountError) as excinfo:
        mount_all(report, root)

    error = excinfo.value
    assert error.mounted == [root, f"{root}/boot"]
    assert f"Could not mount {root}/home" in str(error)
    assert "Mounted before the failure" in str(error)
    # they are still mounted
    assert sorted(mount_by_target(root)) == sorted(
        os.path.realpath(path) for path in error.mounted
    )


def test_umount_tries_everything_when_one_fails(applied, tmp_path):
    """/boot is busy: /home is unmounted anyway, / can not be (/boot is in it)."""
    report, root = applied
    mount_all(report, root)
    busy = f"{root}/boot/busy"
    subprocess.run(["sudo", "mkdir", "-p", busy], check=True)
    subprocess.run(["sudo", "mount", "-t", "tmpfs", "tmpfs", busy], check=True)
    try:
        with pytest.raises(UmountError) as excinfo:
            umount_all(report, root)
        error = excinfo.value
        assert error.result.unmounted == [f"{root}/home"]
        assert [target for target, _ in error.failed] == [f"{root}/boot", root]
        assert all("busy" in reason for _, reason in error.failed)
        assert error.still_mounted == [f"{root}/boot", root]
        assert "Still mounted" in str(error)
    finally:
        subprocess.run(["sudo", "umount", busy], check=False)
    result = umount_all(report, root)
    assert result.unmounted == [f"{root}/boot", root]


def test_the_host_is_bound_for_a_chroot(applied, tmp_path):
    """With fake directories of a host: never bind the real /dev in a test."""
    report, root = applied
    host = tmp_path / "host"
    sources = []
    for relative in BIND_TARGETS:
        (host / relative).mkdir(parents=True, exist_ok=True)
        (host / relative / "marker").write_text(relative)
        sources.append((str(host / relative), relative))

    mounted = mount_all(report, root, bind_system=True, bind_sources=tuple(sources))

    assert mounted == [root, f"{root}/boot", f"{root}/home"] + [
        f"{root}/{relative}" for relative in BIND_TARGETS
    ]
    for relative in BIND_TARGETS:
        assert (os.path.join(root, relative, "marker")).endswith("marker")
        with open(os.path.join(root, relative, "marker"), encoding="utf-8") as marker:
            assert marker.read() == relative
    assert len(mount_by_target(root)) == 3 + 5

    result = umount_all(report, root)

    # the host first, in reverse, then the file systems
    assert result.unmounted == [f"{root}/{rel}" for rel in reversed(BIND_TARGETS)] + [
        f"{root}/home",
        f"{root}/boot",
        root,
    ]
    assert mounted_points(root) == []
    # the fake host is untouched
    assert (host / "dev" / "marker").read_text() == "dev"
