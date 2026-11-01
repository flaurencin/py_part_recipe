"""Mount the file systems of a report under a directory, and unmount them.

The mount points of a report are the ones of the installed system (`/`,
`/boot`, `/boot/efi`...). They are mounted under a root directory chosen here,
like `/target`, to prepare a system before installing it.

- file systems are mounted by UUID, from the shortest mount point to the
  longest, so a parent is always mounted before its children
- swap is never activated, it is not mounted
- a file system that must be mounted but has no UUID stops everything before
  the first mount: skipping it could mount its children over the directories
  of the host
- `bind_system` also binds `/dev`, `/dev/pts`, `/proc`, `/sys` and `/run` of
  the host under the root, what a chroot needs

Mounting stops at the first failure and unmounts nothing: the error lists what
is mounted. Unmounting does the opposite, it goes through every mount point,
from the last mounted to the first, and reports all that failed afterwards.
It never forces nor detaches lazily: a busy mount point stays mounted and is
reported.
"""

import logging
import os
import re
import subprocess
from dataclasses import dataclass, field

from py_part_recipe.common import gen_cmd_for_subprocess
from py_part_recipe.report import Report

LOGGER = logging.getLogger(__name__)

# (directory of the host, mount point under the root), in the order they are mounted
BIND_SOURCES = (
    ("/dev", "dev"),
    ("/dev/pts", "dev/pts"),
    ("/proc", "proc"),
    ("/sys", "sys"),
    ("/run", "run"),
)
MOUNTINFO = "/proc/self/mountinfo"


class MountError(Exception):
    """Mounting failed, `mounted` lists the mount points mounted before."""

    def __init__(self, message: str, mounted: list[str] | None = None) -> None:
        self.mounted = list(mounted or [])
        text = message
        if self.mounted:
            text += f" Mounted before the failure: {', '.join(self.mounted)}."
        super().__init__(text)


class UmountError(Exception):
    """Some mount points could not be unmounted.

    Attributes:
        failed: (mount point, reason) for each one that could not
        still_mounted: what is still mounted, read again after trying all
        result: what was done anyway
    """

    def __init__(
        self,
        failed: list[tuple[str, str]],
        still_mounted: list[str],
        result: "UmountResult",
    ) -> None:
        self.failed = list(failed)
        self.still_mounted = list(still_mounted)
        self.result = result
        details = "; ".join(f"{target}: {reason}" for target, reason in self.failed)
        super().__init__(
            f"Could not unmount {len(self.failed)} mount point(s): {details}. "
            f"Still mounted: {', '.join(self.still_mounted) or 'nothing'}."
        )


@dataclass
class UmountResult:
    unmounted: list[str] = field(default_factory=list)
    not_mounted: list[str] = field(default_factory=list)


# ----- what is mounted


@dataclass(frozen=True)
class MountInfo:
    mount_id: int
    mount_point: str
    fs_type: str
    source: str
    options: str


def _unescape(text: str) -> str:
    """The kernel writes space, tab, newline and backslash as \\040 \\011..."""
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), text)


def parse_mountinfo(text: str) -> list[MountInfo]:
    """The mounts of the content of /proc/self/mountinfo.

    A line is `id parent major:minor root mount-point options [optional...] -
    fs-type source super-options`.
    """
    mounts = []
    for line in text.splitlines():
        left, separator, right = line.partition(" - ")
        fields = left.split(" ")
        if not separator or len(fields) < 6:
            continue
        details = right.split(" ")
        mounts.append(
            MountInfo(
                mount_id=int(fields[0]),
                mount_point=_unescape(fields[4]),
                fs_type=details[0],
                source=_unescape(details[1]) if len(details) > 1 else "",
                options=fields[5],
            )
        )
    return mounts


def current_mounts(path: str = MOUNTINFO) -> list[MountInfo]:
    with open(path, encoding="utf-8") as mountinfo:
        return parse_mountinfo(mountinfo.read())


def _mounted_at(mounts: list[MountInfo]) -> set[str]:
    return {mount.mount_point for mount in mounts}


# ----- what to mount


@dataclass
class MountStep:
    target: str
    command: list[str]
    description: str


def _target(root: str, mount_point: str) -> str:
    return os.path.join(root, mount_point.lstrip("/")) if mount_point != "/" else root


def _check_root(root: str):
    if os.path.realpath(root) == "/":
        raise MountError(
            "Refusing to use / as the root directory, it would act on the "
            "running system."
        )


def plan_mounts(
    report: Report,
    root: str,
    bind_system: bool = False,
    bind_sources: tuple[tuple[str, str], ...] = BIND_SOURCES,
) -> list[MountStep]:
    """The mounts to do, in order. Touches nothing.

    Raises:
        MountError: the root is /, or a file system to mount has no UUID
    """
    _check_root(root)
    steps = []
    for fs in report.mounted_file_systems():
        if not fs.uuid:
            raise MountError(
                f"The file system of volume {fs.volume} ({fs.mount}) has no UUID "
                "and can not be mounted. Nothing was mounted."
            )
        target = _target(root, fs.mount)
        steps.append(
            MountStep(
                target,
                [
                    "mount",
                    "-t",
                    fs.fstab_type,
                    "-o",
                    fs.mount_options,
                    f"UUID={fs.uuid}",
                    target,
                ],
                f"{fs.fstab_type} UUID={fs.uuid}",
            )
        )
    if bind_system:
        for source, relative in bind_sources:
            target = _target(root, relative)
            steps.append(
                MountStep(
                    target, ["mount", "--bind", source, target], f"bind of {source}"
                )
            )
    return steps


def mount_all(
    report: Report,
    root: str,
    bind_system: bool = False,
    bind_sources: tuple[tuple[str, str], ...] = BIND_SOURCES,
) -> list[str]:
    """Mount the file systems of the report under `root`, return the mount
    points in the order they were mounted.

    Raises:
        MountError: nothing is mounted if the plan is not possible or if
            something is already mounted at or under the root; if a mount
            fails the ones done before stay mounted, the error lists them
    """
    steps = plan_mounts(report, root, bind_system, bind_sources)
    real_root = os.path.realpath(root)
    for mount in current_mounts():
        if mount.mount_point == real_root or mount.mount_point.startswith(
            real_root + "/"
        ):
            raise MountError(
                f"{mount.mount_point} is already mounted, nothing was mounted."
            )
    mounted: list[str] = []
    for step in steps:
        for command, what in (
            (["mkdir", "-p", step.target], "create"),
            (step.command, "mount"),
        ):
            result = _run(command)
            if result is not None:
                raise MountError(
                    f"Could not {what} {step.target} ({step.description}): "
                    f"{result}.",
                    mounted,
                )
        mounted.append(step.target)
    return mounted


# ----- unmounting


def _run(command: list[str]) -> str | None:
    """None when it worked, else why it did not."""
    result = subprocess.run(
        gen_cmd_for_subprocess(command), capture_output=True, check=False
    )
    if result.returncode == 0:
        return None
    error = result.stderr.decode("utf-8", errors="replace").strip()
    return error or f"exit code {result.returncode}"


def umount_targets(
    report: Report,
    root: str,
    bind_sources: tuple[tuple[str, str], ...] = BIND_SOURCES,
) -> list[str]:
    """The mount points to unmount, the last one mounted first: the bind
    mounts of the host, then the file systems from the longest mount point to
    the shortest."""
    _check_root(root)
    # the order they are mounted in: the file systems, then the host
    targets = [_target(root, fs.mount) for fs in report.mounted_file_systems()]
    targets += [_target(root, relative) for _, relative in bind_sources]
    return list(reversed(targets))


def umount_all(report: Report, root: str) -> UmountResult:
    """Unmount what `mount_all` mounted under `root`.

    A mount point that is not mounted is skipped, so it can be run twice. All
    the mount points are tried even if one fails.

    Raises:
        UmountError: some could not be unmounted (busy...), nothing is forced
    """
    targets = umount_targets(report, root)
    mounted_now = _mounted_at(current_mounts())
    result = UmountResult()
    failed: list[tuple[str, str]] = []
    for target in targets:
        if os.path.realpath(target) not in mounted_now:
            result.not_mounted.append(target)
            continue
        error = _run(["umount", target])
        if error is None:
            result.unmounted.append(target)
        else:
            failed.append((target, error))
    if failed:
        remaining = _mounted_at(current_mounts())
        still_mounted = [
            target for target in targets if os.path.realpath(target) in remaining
        ]
        raise UmountError(failed, still_mounted, result)
    return result
