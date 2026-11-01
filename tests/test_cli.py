import io
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from device_guard import ensure_vg_absent
from device_helpers import signatures
from samples import DISKS, RECIPE, sample_report

from py_part_recipe.cli import (
    EXIT_APPLY_FAILED,
    EXIT_INVALID,
    EXIT_MOUNT_FAILED,
    EXIT_NOT_CONFIRMED,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from py_part_recipe.report import Report
from py_part_recipe.wipe import wipe_disk

EXAMPLE = "examples/example_recipe.yml"


def write_yaml(tmp_path, data, name="file.yml") -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return str(path)


def run(capsys, *argv):
    """The exit code, what is written on stdout and on stderr."""
    code = main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


@pytest.fixture
def disks():
    """Disks with nothing on them before and after, and the raid stopped."""
    ensure_vg_absent("vg_test")
    for disk in DISKS:
        wipe_disk(disk)
    yield DISKS
    for disk in DISKS:
        wipe_disk(disk)


# ----- command line


def test_no_command(capsys):
    code, out, err = run(capsys)
    assert code == EXIT_USAGE and out == ""
    assert "usage:" in err


@pytest.mark.parametrize(
    "argv",
    (
        ["nothing"],
        ["validate"],
        ["validate", "a.yml", "b.yml"],
        ["apply", "recipe.yml", "--format", "xml"],
        ["apply", "recipe.yml", "--unknown"],
        ["fstab"],
    ),
)
def test_wrong_command_line(capsys, argv):
    code, out, err = run(capsys, *argv)
    assert code == EXIT_USAGE and out == ""
    assert "error:" in err


def test_help(capsys):
    code, out, err = run(capsys, "--help")
    assert code == EXIT_OK and err == ""
    for command in ("validate", "apply", "fstab"):
        assert command in out


def test_apply_help_warns_about_data(capsys):
    code, out, _ = run(capsys, "apply", "--help")
    assert code == EXIT_OK
    assert "NOTHING IS UNDONE" in out


# ----- validate


def test_validate_example(capsys):
    code, out, err = run(capsys, "validate", EXAMPLE)
    assert code == EXIT_OK and err == ""
    assert out == (
        f"{EXAMPLE} is valid: 3 group(s) of disks, 12 volume(s), 6 file system(s)\n"
    )


def test_validate_reports_every_problem(capsys, tmp_path):
    data = yaml.safe_load(Path(EXAMPLE).read_text())
    data["partitioning"][0]["recipe"]["partitions"][0]["weight"] = 0
    data["volumes"][2]["params"]["level"] = 3
    path = write_yaml(tmp_path, data)
    code, out, err = run(capsys, "validate", path)
    assert code == EXIT_INVALID and out == ""
    assert "2 problem(s)" in err
    assert "partitioning[0].recipe.partitions[0].weight" in err
    assert "volumes[2].params.level" in err


def test_validate_unreadable_file(capsys, tmp_path):
    code, out, err = run(capsys, "validate", str(tmp_path / "missing.yml"))
    assert code == EXIT_INVALID and out == ""
    assert "cannot read" in err and "missing.yml" in err


def test_validate_not_yaml(capsys, tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("partitioning: [unclosed")
    code, _, err = run(capsys, "validate", str(path))
    assert code == EXIT_INVALID and "not valid yaml" in err


# ----- apply: what is refused before anything is written


def disks_state():
    return [signatures(disk) for disk in DISKS]


def test_apply_invalid_recipe(capsys, tmp_path):
    data = yaml.safe_load(Path(EXAMPLE).read_text())
    data["file_systems"][0]["type"] = "zfs"
    before = disks_state()
    code, out, err = run(capsys, "apply", write_yaml(tmp_path, data), "--yes")
    assert code == EXIT_INVALID and out == ""
    assert "file_systems[0].type" in err
    assert disks_state() == before


@pytest.mark.parametrize("answer", ("", "no\n", "y\n", "yess\n"))
def test_apply_not_confirmed(capsys, monkeypatch, tmp_path, answer):
    monkeypatch.setattr(sys, "stdin", io.StringIO(answer))
    before = disks_state()
    code, out, err = run(capsys, "apply", write_yaml(tmp_path, RECIPE))
    assert code == EXIT_NOT_CONFIRMED
    assert out == ""  # nothing that could be mistaken for a report
    assert "Not confirmed, nothing was applied." in err
    assert disks_state() == before


def test_apply_tells_which_disks_it_changes(capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "stdin", io.StringIO("no\n"))
    _, _, err = run(capsys, "apply", write_yaml(tmp_path, RECIPE))
    for disk in DISKS:
        assert f"{disk}  (wiped, then repartitioned)" in err
    assert "data they hold is lost" in err and "nothing is undone" in err


def test_apply_describes_disks_by_mode(capsys, monkeypatch, tmp_path):
    data = yaml.safe_load(yaml.safe_dump(RECIPE))
    data["partitioning"][0]["recipe"]["wipe"] = False
    data["partitioning"][1]["recipe"]["wipe"] = False
    data["partitioning"][1]["recipe"]["keep_partitions"] = True
    monkeypatch.setattr(sys, "stdin", io.StringIO("no\n"))
    _, _, err = run(capsys, "apply", write_yaml(tmp_path, data))
    assert "/dev/loop100  (repartitioned, new partition table)" in err
    assert "/dev/loop102  (partitions added in the free space" in err


def test_apply_refuses_an_output_that_can_not_be_written(capsys, tmp_path):
    """Once the disks are changed it is too late to find out."""
    before = disks_state()
    output = str(tmp_path / "no_such_directory" / "report.yml")
    code, out, err = run(
        capsys, "apply", write_yaml(tmp_path, RECIPE), "--yes", "-o", output
    )
    assert code == EXIT_INVALID and out == ""
    assert "nothing was applied" in err
    assert disks_state() == before


def test_apply_refuses_an_output_that_is_a_directory(capsys, tmp_path):
    before = disks_state()
    code, _, err = run(
        capsys, "apply", write_yaml(tmp_path, RECIPE), "--yes", "-o", str(tmp_path)
    )
    assert code == EXIT_INVALID and "nothing was applied" in err
    assert disks_state() == before


# ----- apply: for real, on the loop devices


def test_apply_to_a_file_then_fstab(capsys, disks, tmp_path):
    recipe = write_yaml(tmp_path, RECIPE)
    output = str(tmp_path / "report.yml")

    code, out, err = run(capsys, "apply", recipe, "--yes", "-o", output)

    assert code == EXIT_OK
    assert out == ""
    assert f"Report written to {output}" in err
    report = Report.load(output)
    assert sorted(report.volumes) == [
        "boot_vol",
        "lv_home",
        "lv_root",
        "md_vol",
        "swap_vol",
        "vg_test",
    ]
    assert signatures("/dev/vg_test/lv_root")["TYPE"] == "ext4"

    code, out, err = run(capsys, "fstab", output)
    assert code == EXIT_OK and err == ""
    assert out == report.fstab()
    assert len(out.splitlines()) == 4
    assert out.splitlines()[0].endswith("\t/\text4\tdefaults\t0\t1")


def test_apply_prints_the_report_on_stdout(capsys, disks, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.StringIO("yes\n"))  # the answer is asked

    code, out, err = run(
        capsys, "apply", write_yaml(tmp_path, RECIPE), "--format", "json"
    )

    assert code == EXIT_OK
    # stdout is the report and nothing else, the questions went to stderr
    assert Report.from_dict(json.loads(out)).version == 1
    assert "Type 'yes' to continue" in err and "/dev/loop100" in err
    assert "Report written" not in err


def test_apply_output_json_by_extension(capsys, disks, tmp_path):
    output = tmp_path / "report.json"
    code, _, _ = run(
        capsys, "apply", write_yaml(tmp_path, RECIPE), "--yes", "-o", str(output)
    )
    assert code == EXIT_OK
    assert output.read_text().lstrip().startswith("{")
    assert Report.load(str(output)).file_systems


def test_apply_failure(capsys, disks, tmp_path):
    data = yaml.safe_load(yaml.safe_dump(RECIPE))
    data["file_systems"][3]["type"] = "xfs"  # does not fit 8 MiB
    output = tmp_path / "report.yml"

    code, out, err = run(
        capsys, "apply", write_yaml(tmp_path, data), "--yes", "-o", str(output)
    )

    assert code == EXIT_APPLY_FAILED
    assert out == "" and not output.exists()
    assert "stage 'file_systems'" in err
    assert "Nothing was undone" in err
    assert "Volumes built: md_vol, vg_test, lv_root, lv_home" in err
    assert "File systems formatted: lv_root, lv_home, boot_vol" in err


# ----- fstab


def test_fstab(capsys, tmp_path):
    report = sample_report()
    path = tmp_path / "report.yml"
    path.write_text(report.dumps("yaml"))
    code, out, err = run(capsys, "fstab", str(path))
    assert code == EXIT_OK and err == ""
    assert out == report.fstab()
    assert out.splitlines()[0] == "UUID=uuid-root\t/\text4\tdefaults\t0\t1"


def test_fstab_of_a_json_report(capsys, tmp_path):
    path = tmp_path / "report.json"
    path.write_text(sample_report().dumps("json"))
    code, out, _ = run(capsys, "fstab", str(path))
    assert code == EXIT_OK and out == sample_report().fstab()


def test_fstab_warns_about_a_file_system_without_uuid(capsys, tmp_path):
    report = sample_report()
    report.file_systems[0].uuid = None
    path = tmp_path / "report.yml"
    path.write_text(report.dumps("yaml"))
    code, out, err = run(capsys, "fstab", str(path))
    assert code == EXIT_OK
    assert "/var" not in out  # the file system without uuid
    assert "warning:" in err and "volume_lv_var" in err


def test_fstab_invalid_report(capsys, tmp_path):
    data = sample_report().to_dict()
    data["file_systems"][0]["volume"] = "nowhere"
    data["file_systems"][1]["type"] = "zfs"
    code, out, err = run(capsys, "fstab", write_yaml(tmp_path, data))
    assert code == EXIT_INVALID and out == ""
    assert "2 problem(s)" in err and "nowhere" in err


def test_fstab_unreadable_report(capsys, tmp_path):
    code, out, err = run(capsys, "fstab", str(tmp_path / "missing.yml"))
    assert code == EXIT_INVALID and out == ""
    assert "cannot read" in err


# ----- the entry points


def test_run_as_a_module():
    result = subprocess.run(
        [sys.executable, "-m", "py_part_recipe", "validate", EXAMPLE],
        capture_output=True,
    )
    assert result.returncode == 0
    assert b"is valid" in result.stdout


def test_exit_code_of_the_module():
    result = subprocess.run(
        [sys.executable, "-m", "py_part_recipe", "validate", "missing.yml"],
        capture_output=True,
    )
    assert result.returncode == EXIT_INVALID


def test_installed_command():
    command = Path(sys.executable).parent / "py-part-recipe"
    assert command.exists(), "run uv sync to install the command"
    result = subprocess.run([str(command), "validate", EXAMPLE], capture_output=True)
    assert result.returncode == 0


# ----- mount and umount


def sample_report_file(tmp_path, report=None) -> str:
    path = tmp_path / "report.yml"
    path.write_text((report or sample_report()).dumps("yaml"))
    return str(path)


@pytest.mark.parametrize(
    "argv",
    (
        ["mount", "report.yml"],
        ["umount", "report.yml"],
        ["mount", "--root", "/target"],
        ["umount", "--root", "/target"],
        ["umount", "report.yml", "--root", "/target", "--bind-system"],
    ),
)
def test_mount_wrong_command_line(capsys, argv):
    code, out, err = run(capsys, *argv)
    assert code == EXIT_USAGE and out == "" and "error:" in err


@pytest.mark.parametrize("command", ("mount", "umount"))
def test_mount_help(capsys, command):
    code, out, _ = run(capsys, command, "--help")
    assert code == EXIT_OK and "--root" in out


@pytest.mark.parametrize("command", ("mount", "umount"))
def test_mount_unreadable_report(capsys, tmp_path, command):
    code, out, err = run(
        capsys, command, str(tmp_path / "missing.yml"), "--root", str(tmp_path)
    )
    assert code == EXIT_INVALID and out == "" and "cannot read" in err


@pytest.mark.parametrize("command", ("mount", "umount"))
def test_mount_invalid_report(capsys, tmp_path, command):
    data = sample_report().to_dict()
    data["file_systems"][0]["volume"] = "nowhere"
    code, out, err = run(
        capsys, command, write_yaml(tmp_path, data), "--root", str(tmp_path / "t")
    )
    assert code == EXIT_INVALID and out == "" and "nowhere" in err


@pytest.mark.parametrize("command", ("mount", "umount"))
def test_mount_never_on_the_root_of_the_system(capsys, tmp_path, command):
    code, out, err = run(capsys, command, sample_report_file(tmp_path), "--root", "/")
    assert code == EXIT_MOUNT_FAILED and out == ""
    assert "Refusing to use /" in err


def test_mount_a_file_system_without_uuid_mounts_nothing(capsys, tmp_path):
    report = sample_report()
    report.file_systems[3].uuid = None
    root = tmp_path / "target"
    code, out, err = run(
        capsys, "mount", sample_report_file(tmp_path, report), "--root", str(root)
    )
    assert code == EXIT_MOUNT_FAILED and out == ""
    assert "no UUID" in err and "Nothing was mounted" in err
    assert not root.exists()


def test_mount_where_something_is_mounted(capsys, tmp_path):
    code, out, err = run(
        capsys, "mount", sample_report_file(tmp_path), "--root", "/proc"
    )
    assert code == EXIT_MOUNT_FAILED and out == ""
    assert "/proc is already mounted" in err


def test_umount_what_is_not_mounted(capsys, tmp_path):
    root = str(tmp_path / "target")
    code, out, err = run(capsys, "umount", sample_report_file(tmp_path), "--root", root)
    assert code == EXIT_OK and err == ""
    lines = out.splitlines()
    assert len(lines) == 4 + 5  # the file systems and the host
    assert all(line.startswith("not mounted ") for line in lines)
    assert lines[0] == f"not mounted {root}/run"  # the last mounted comes first


def test_mount_and_umount_commands(capsys, applied, tmp_path):
    report, root = applied
    path = sample_report_file(tmp_path, report)

    code, out, err = run(capsys, "mount", path, "--root", root)

    assert code == EXIT_OK and err == ""
    assert out.splitlines() == [
        f"mounted {root}",
        f"mounted {root}/boot",
        f"mounted {root}/home",
    ]

    code, out, err = run(capsys, "mount", path, "--root", root)
    assert code == EXIT_MOUNT_FAILED and out == ""
    assert "already mounted" in err

    code, out, err = run(capsys, "umount", path, "--root", root)

    assert code == EXIT_OK and err == ""
    lines = out.splitlines()
    assert lines[:3] == [
        f"unmounted {root}/home",
        f"unmounted {root}/boot",
        f"unmounted {root}",
    ]
    assert len(lines) == 3 + 5 and all(
        line.startswith("not mounted ") for line in lines[3:]
    )

    code, out, err = run(capsys, "umount", path, "--root", root)
    assert code == EXIT_OK and len(out.splitlines()) == 8


def test_umount_command_reports_what_is_busy(capsys, applied, tmp_path):
    import subprocess

    report, root = applied
    path = sample_report_file(tmp_path, report)
    assert run(capsys, "mount", path, "--root", root)[0] == EXIT_OK
    busy = f"{root}/boot/busy"
    subprocess.run(["sudo", "mkdir", "-p", busy], check=True)
    subprocess.run(["sudo", "mount", "-t", "tmpfs", "tmpfs", busy], check=True)
    try:
        code, out, err = run(capsys, "umount", path, "--root", root)
    finally:
        subprocess.run(["sudo", "umount", busy], check=False)

    assert code == EXIT_MOUNT_FAILED
    assert f"unmounted {root}/home" in out
    assert "Could not unmount 2 mount point(s)" in err
    assert f"Still mounted: {root}/boot, {root}" in err
    assert run(capsys, "umount", path, "--root", root)[0] == EXIT_OK
