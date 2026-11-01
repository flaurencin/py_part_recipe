import os
import subprocess

import parted
import pytest

import device_guard
from device_guard import DeviceGuardError

FORBIDDEN = (
    "/dev/sda",
    "/dev/nvme0n1p1",
    "/dev/loop0",
    "/dev/loop107",
    "/dev/loop119",
    "/dev/loop122",
    "/dev/md0",
)
ALLOWED = (
    "/dev/loop100",
    "/dev/loop106p1",
    "/dev/loop120",
    "/dev/loop121p2",
    "/dev/md100",
    "/dev/md199",
    "/dev/vg_test",
    "/dev/vg_test/lv_test",
    "/dev/null",
)


@pytest.mark.parametrize("path", FORBIDDEN)
def test_path_refused(path):
    with pytest.raises(DeviceGuardError, match=path):
        device_guard.check_device_path(path, "test")


@pytest.mark.parametrize("path", ALLOWED + ("/tmp/loop0", "relative", None))
def test_path_allowed(path):
    device_guard.check_device_path(path, "test")


@pytest.mark.parametrize("path", FORBIDDEN)
def test_path_traversal_is_refused(path):
    with pytest.raises(DeviceGuardError):
        device_guard.check_device_path(f"/dev/loop100/../{path[5:]}", "test")


@pytest.mark.parametrize("path", FORBIDDEN)
def test_subprocess_refused(path):
    with pytest.raises(DeviceGuardError):
        subprocess.run(["true", path])
    with pytest.raises(DeviceGuardError):
        subprocess.run(["true", f"--device={path}"])
    with pytest.raises(DeviceGuardError):
        subprocess.run(f"true {path}", shell=True)
    with pytest.raises(DeviceGuardError):
        subprocess.check_output(["true", path])


@pytest.mark.parametrize("path", ALLOWED)
def test_subprocess_allowed(path):
    assert subprocess.run(["true", path]).returncode == 0


@pytest.mark.parametrize("path", FORBIDDEN)
def test_parted_refused(path):
    with pytest.raises(DeviceGuardError):
        parted.Device(path)
    with pytest.raises(DeviceGuardError):
        parted.getDevice(path)


@pytest.mark.parametrize("path", FORBIDDEN)
@pytest.mark.parametrize("mode", ("r+b", "wb", "ab", "w"))
def test_open_for_writing_refused(path, mode):
    with pytest.raises(DeviceGuardError):
        open(path, mode)


def test_open_for_reading_is_not_checked():
    with open("/dev/null", "rb") as dev:
        assert dev.read() == b""


@pytest.mark.parametrize(
    "name", ("root", "vg00-root", "dm-0", "vg_test", "vg_test-lv_test", "vg_testing-lv")
)
def test_dmsetup_remove(name):
    allowed = name.startswith("vg_test")
    command = ["true", "dmsetup", "remove", "--force", name]
    if allowed:
        assert subprocess.run(command).returncode == 0
    else:
        with pytest.raises(DeviceGuardError, match=name):
            subprocess.run(command)


@pytest.mark.parametrize("sub", ("remove_all", "wipe_table", "suspend", "create"))
def test_dmsetup_other_commands_refused(sub):
    with pytest.raises(DeviceGuardError, match=sub):
        subprocess.run(["true", "dmsetup", sub, "vg_test"])


@pytest.mark.parametrize("sub", ("ls", "info", "status"))
def test_dmsetup_read_only_commands_allowed(sub):
    assert subprocess.run(["true", "dmsetup", sub]).returncode == 0


# ----- mounting: only test devices, only inside the temporary directory


def check(*command):
    """Check a command as the guard does when it is run, without running it."""
    device_guard._check_command(list(command), False, "test")


@pytest.mark.parametrize("prefix", ([], ["sudo"], ["sudo", "-n"]))
def test_mkdir_inside_the_temporary_directory(tmp_path, prefix):
    check(*prefix, "mkdir", "-p", str(tmp_path / "target" / "boot"))


@pytest.mark.parametrize("prefix", ([], ["sudo"], ["sudo", "-n"]))
@pytest.mark.parametrize("path", ("/etc/x", "/", "/target", "/mnt/target"))
def test_mkdir_outside_the_temporary_directory(prefix, path):
    with pytest.raises(DeviceGuardError, match="refusing to create"):
        check(*prefix, "mkdir", "-p", path)


def test_the_temporary_directory_itself_is_not_a_target(tmp_path):
    import tempfile

    with pytest.raises(DeviceGuardError, match="refusing to create"):
        check("mkdir", "-p", tempfile.gettempdir())


def test_a_path_leaving_the_temporary_directory(tmp_path):
    import tempfile

    leaving = os.path.join(tempfile.gettempdir(), "..", "etc", "x")
    with pytest.raises(DeviceGuardError, match="refusing to create"):
        check("mkdir", "-p", leaving)
    # going down and coming back is the same place
    check("mkdir", "-p", str(tmp_path / "a" / ".." / "b"))


@pytest.mark.parametrize("prefix", ([], ["sudo"]))
def test_umount(tmp_path, prefix):
    check(*prefix, "umount", str(tmp_path / "target" / "boot"))
    check(*prefix, "umount", "/dev/loop100p1")
    with pytest.raises(DeviceGuardError, match="refusing to unmount /boot"):
        check(*prefix, "umount", "/boot")
    with pytest.raises(DeviceGuardError, match="refusing to unmount /"):
        check(*prefix, "umount", "/")


@pytest.mark.parametrize("option", ("-a", "-l", "-f", "-R", "-A", "--all"))
def test_umount_dangerous_options(tmp_path, option):
    with pytest.raises(DeviceGuardError, match="not allowed"):
        check("umount", option, str(tmp_path))


@pytest.mark.parametrize("prefix", ([], ["sudo"]))
def test_mount_a_tmpfs_or_bind_inside_the_temporary_directory(tmp_path, prefix):
    (tmp_path / "host").mkdir()
    check(*prefix, "mount", "-t", "tmpfs", "tmpfs", str(tmp_path / "x"))
    check(*prefix, "mount", "--bind", str(tmp_path / "host"), str(tmp_path / "x"))
    check(*prefix, "mount", "-B", str(tmp_path / "host"), str(tmp_path / "x"))


@pytest.mark.parametrize("source", ("/dev", "/proc", "/sys", "/run", "/etc", "/"))
@pytest.mark.parametrize("option", ("--bind", "-B", "--rbind"))
def test_bind_of_a_directory_of_the_system(tmp_path, source, option):
    """A bind of /dev left behind would be walked into by the cleanup."""
    with pytest.raises(DeviceGuardError, match="refusing to bind"):
        check("mount", option, source, str(tmp_path / "x"))


@pytest.mark.parametrize("target", ("/mnt", "/target", "/", "/boot/efi"))
def test_mount_outside_the_temporary_directory(target):
    with pytest.raises(DeviceGuardError, match="refusing to mount on"):
        check("mount", "-t", "tmpfs", "tmpfs", target)


def test_mount_a_test_device_by_path(tmp_path):
    check("mount", "-t", "ext4", "/dev/loop102p1", str(tmp_path / "x"))
    check("mount", "-o", "noatime", "/dev/md100", str(tmp_path / "x"))
    with pytest.raises(DeviceGuardError, match="refusing to use /dev/sda1"):
        check("mount", "/dev/sda1", str(tmp_path / "x"))


@pytest.mark.parametrize(
    "source", ("LABEL=root", "PARTUUID=1234", "proc", "/etc", "server:/share")
)
def test_mount_other_sources(tmp_path, source):
    with pytest.raises(DeviceGuardError, match="refusing to mount"):
        check("mount", source, str(tmp_path / "x"))


def test_mount_by_a_uuid_nobody_has(tmp_path):
    with pytest.raises(DeviceGuardError, match="no device has this uuid"):
        check(
            "mount",
            "-t",
            "ext4",
            "UUID=00000000-0000-0000-0000-000000000000",
            str(tmp_path / "x"),
        )


@pytest.mark.parametrize("command", (["mount"], ["mount", "-a"], ["mount", "/mnt/x"]))
def test_mount_from_the_fstab_or_listing(command):
    with pytest.raises(DeviceGuardError):
        check(*command)


@pytest.mark.parametrize("option", ("-a", "--all", "-l", "-f"))
def test_mount_dangerous_options(tmp_path, option):
    with pytest.raises(DeviceGuardError, match="not allowed"):
        check("mount", option, "tmpfs", str(tmp_path / "x"))


def test_the_guard_refuses_a_mount_when_it_is_run(tmp_path):
    with pytest.raises(DeviceGuardError, match="refusing to mount on /mnt"):
        subprocess.run(["mount", "-t", "tmpfs", "tmpfs", "/mnt"])
    with pytest.raises(DeviceGuardError, match="refusing to bind /dev"):
        subprocess.run(["sudo", "mount", "--bind", "/dev", str(tmp_path)])


def test_other_programs_are_not_taken_for_mount(tmp_path):
    # "mount" as an argument is not the program
    assert subprocess.run(["true", "mount", "/mnt", "umount"]).returncode == 0


def test_environment_has_no_problem():
    assert list(device_guard.verify_environment()) == []
