"""The `py-part-recipe` command.

    py-part-recipe validate recipe.yml
    py-part-recipe apply recipe.yml [--yes] [--format yaml|json] [--output FILE]
    py-part-recipe fstab report.yml
    py-part-recipe mount report.yml --root /target [--bind-system]
    py-part-recipe umount report.yml --root /target

What a command produces goes to stdout (the report, the fstab), everything
else (questions, summaries, errors, warnings) goes to stderr, so the output
can be redirected or piped.

Exit codes:

    0    done
    1    the recipe or report is not valid, or a file can not be read
    2    applying the recipe failed: nothing is undone, the error says what
         was already changed
    3    not applied: the confirmation was not given
    4    mounting or unmounting failed: what is still mounted is listed
    64   wrong command line
    130  interrupted while applying: the state of the disks is not known
"""

import argparse
import logging
import os
import sys

from py_part_recipe.applier import ApplyError, apply_recipe
from py_part_recipe.mounting import MountError, UmountError, mount_all, umount_all
from py_part_recipe.recipe_loader import RecipeError, RecipeSpec, load_recipe
from py_part_recipe.report import Report, ReportError

EXIT_OK = 0
EXIT_INVALID = 1
EXIT_APPLY_FAILED = 2
EXIT_NOT_CONFIRMED = 3
EXIT_MOUNT_FAILED = 4
EXIT_USAGE = 64
EXIT_INTERRUPTED = 130


def _error(message: str):
    print(f"error: {message}", file=sys.stderr)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse exits with 2, which means "applying failed" here
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="py-part-recipe",
        description="Partition disks and build RAID, LVM and file systems "
        "from a yaml recipe.",
    )
    commands = parser.add_subparsers(
        dest="command", required=True, parser_class=_Parser, metavar="command"
    )

    validate = commands.add_parser(
        "validate",
        help="check a recipe, no disk is touched",
        description="Read a recipe and report every problem found. Touches "
        "no disk and needs no privileges.",
    )
    validate.add_argument("recipe", help="the recipe, a yaml file")

    apply = commands.add_parser(
        "apply",
        help="apply a recipe: DESTROYS the data of the disks it uses",
        description="Partition the disks, build the volumes and format the "
        "file systems of a recipe, then write the report of what was created "
        "(yaml or json). Needs root or sudo. NOTHING IS UNDONE if it fails.",
    )
    apply.add_argument("recipe", help="the recipe, a yaml file")
    apply.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="do not ask for confirmation",
    )
    apply.add_argument(
        "-f",
        "--format",
        choices=("yaml", "json"),
        help="format of the report (default: from the extension of --output, "
        "else yaml)",
    )
    apply.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="write the report to FILE instead of the standard output",
    )

    fstab = commands.add_parser(
        "fstab",
        help="print the fstab of a report",
        description="Read the report written by apply and print an fstab, "
        "the file systems are named by UUID.",
    )
    fstab.add_argument("report", help="the report, a yaml or json file")

    mount = commands.add_parser(
        "mount",
        help="mount the file systems of a report under a directory",
        description="Mount the file systems of the report written by apply "
        "under ROOT, by UUID, from the shortest mount point to the longest "
        "(/target, /target/boot...). Swap is not activated. Stops at the first "
        "failure and unmounts nothing. Needs root or sudo.",
    )
    mount.add_argument("report", help="the report, a yaml or json file")
    mount.add_argument(
        "-r",
        "--root",
        required=True,
        metavar="DIR",
        help="the directory to mount under, like /target",
    )
    mount.add_argument(
        "--bind-system",
        action="store_true",
        help="also bind /dev, /dev/pts, /proc, /sys and /run of this system "
        "under ROOT, what a chroot needs",
    )

    umount = commands.add_parser(
        "umount",
        help="unmount what mount mounted",
        description="Unmount the bind mounts and the file systems of the "
        "report under ROOT, the last mounted first. What is not mounted is "
        "skipped. Every mount point is tried even if one fails (busy...), "
        "nothing is forced. Needs root or sudo.",
    )
    umount.add_argument("report", help="the report, a yaml or json file")
    umount.add_argument(
        "-r",
        "--root",
        required=True,
        metavar="DIR",
        help="the directory the file systems were mounted under",
    )
    return parser


# ----- validate


def _read_recipe(path: str) -> RecipeSpec | None:
    """The recipe, or None after telling why it can not be used."""
    try:
        return load_recipe(path)
    except RecipeError as error:
        _error(f"{path} is not a valid recipe, {len(error.errors)} problem(s):")
        for problem in error.errors:
            print(f"  - {problem}", file=sys.stderr)
    except OSError as error:
        _error(f"cannot read {path}: {error.strerror or error}")
    return None


def _validate(args) -> int:
    recipe = _read_recipe(args.recipe)
    if recipe is None:
        return EXIT_INVALID
    print(
        f"{args.recipe} is valid: {len(recipe.partitioning)} group(s) of disks, "
        f"{len(recipe.volumes)} volume(s), {len(recipe.file_systems)} file system(s)"
    )
    return EXIT_OK


# ----- apply


def _describe_disks(recipe: RecipeSpec) -> list[str]:
    lines = []
    for group in recipe.partitioning:
        if group.wipe:
            what = "wiped, then repartitioned"
        elif group.keep_partitions:
            what = "partitions added in the free space, the others are kept"
        else:
            what = "repartitioned, new partition table"
        lines.extend(f"  {device}  ({what})" for device in group.devices)
    return lines


def _confirmed(recipe: RecipeSpec) -> bool:
    print("Applying this recipe changes these disks:", file=sys.stderr)
    for line in _describe_disks(recipe):
        print(line, file=sys.stderr)
    print(
        "\nThe data they hold is lost and nothing is undone if it fails.",
        file=sys.stderr,
    )
    sys.stderr.write("Type 'yes' to continue: ")
    sys.stderr.flush()
    try:
        answer = input()
    except EOFError:
        answer = ""
    return answer.strip().lower() == "yes"


def _output_is_writable(path: str) -> bool:
    """Check before applying, once the disks are changed it is too late."""
    if os.path.exists(path):
        return os.path.isfile(path) and os.access(path, os.W_OK)
    return os.access(os.path.dirname(os.path.abspath(path)), os.W_OK)


def _apply(args) -> int:
    recipe = _read_recipe(args.recipe)
    if recipe is None:
        return EXIT_INVALID
    fmt = args.format or (
        "json" if args.output and args.output.endswith(".json") else "yaml"
    )
    if args.output and not _output_is_writable(args.output):
        _error(f"cannot write the report to {args.output}, nothing was applied")
        return EXIT_INVALID
    if not args.yes and not _confirmed(recipe):
        print("Not confirmed, nothing was applied.", file=sys.stderr)
        return EXIT_NOT_CONFIRMED
    try:
        report = apply_recipe(recipe)
    except ApplyError as error:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_APPLY_FAILED
    except KeyboardInterrupt:
        _error("interrupted, nothing was undone and the state of the disks is unknown")
        return EXIT_INTERRUPTED
    text = report.dumps(fmt)
    if not args.output:
        sys.stdout.write(text)
        return EXIT_OK
    try:
        with open(args.output, "w", encoding="utf-8") as report_file:
            report_file.write(text)
    except OSError as error:
        # the recipe is applied: never lose the report
        _error(f"cannot write {args.output} ({error.strerror}), the report follows")
        sys.stdout.write(text)
        return EXIT_INVALID
    print(f"Report written to {args.output}", file=sys.stderr)
    return EXIT_OK


# ----- fstab


def _read_report(path: str) -> Report | None:
    """The report, or None after telling why it can not be used."""
    try:
        return Report.load(path)
    except ReportError as error:
        _error(f"{path} is not a valid report, {len(error.errors)} problem(s):")
        for problem in error.errors:
            print(f"  - {problem}", file=sys.stderr)
    except OSError as error:
        _error(f"cannot read {path}: {error.strerror or error}")
    return None


def _fstab(args) -> int:
    report = _read_report(args.report)
    if report is None:
        return EXIT_INVALID
    sys.stdout.write(report.fstab())
    return EXIT_OK


# ----- mount and umount


def _mount(args) -> int:
    report = _read_report(args.report)
    if report is None:
        return EXIT_INVALID
    try:
        mounted = mount_all(report, args.root, bind_system=args.bind_system)
    except MountError as error:
        _error(str(error))
        return EXIT_MOUNT_FAILED
    for target in mounted:
        print(f"mounted {target}")
    return EXIT_OK


def _umount(args) -> int:
    report = _read_report(args.report)
    if report is None:
        return EXIT_INVALID
    try:
        result = umount_all(report, args.root)
    except MountError as error:  # the root is /
        _error(str(error))
        return EXIT_MOUNT_FAILED
    except UmountError as error:
        for target in error.result.unmounted:
            print(f"unmounted {target}")
        for target in error.result.not_mounted:
            print(f"not mounted {target}")
        _error(str(error))
        return EXIT_MOUNT_FAILED
    for target in result.unmounted:
        print(f"unmounted {target}")
    for target in result.not_mounted:
        print(f"not mounted {target}")
    return EXIT_OK


COMMANDS = {
    "validate": _validate,
    "apply": _apply,
    "fstab": _fstab,
    "mount": _mount,
    "umount": _umount,
}


def main(argv: list[str] | None = None) -> int:
    """Run the command line, return the exit code."""
    try:
        args = _build_parser().parse_args(argv)
    except SystemExit as stop:  # usage error, or --help which exits with 0
        return stop.code if isinstance(stop.code, int) else EXIT_OK
    # warnings of the library (a file system without UUID...) go to stderr
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("warning: %(message)s"))
    handler.setLevel(logging.WARNING)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        return COMMANDS[args.command](args)
    finally:
        root.removeHandler(handler)


if __name__ == "__main__":
    sys.exit(main())
