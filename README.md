# py_part_recipe

Declarative disk layout for Linux. You describe partitions, volumes (RAID, LVM)
and file systems in a YAML *recipe*, and the library computes the layout and
applies it with `libparted`, `mdadm`, LVM tools and `mkfs.*`.

> **Status: early development (v0.1.0).** The building blocks (partition sizing,
> partitioning, wiping, RAID, LVM, formatting, the report) exist and are tested,
> a recipe file can be read and validated (`load_recipe`) and applied
> (`apply_recipe`), which returns the report, and there is a [command
> line](#command-line) to validate, apply, print an fstab, and mount and
> unmount the result under a directory. See [Project status](#project-status).

> **Warning:** this software repartitions and formats block devices. Wrong
> input destroys data. Use it on disposable disks or loop devices until it
> matures. See [Responsibility and failure handling](#responsibility-and-failure-handling).

## Design choices

- **GPT only.** This is a modern tool: MBR/msdos tables, and the extended and
  logical partitions that go with them, are not supported.
- **Two steps.** A recipe is first *parsed and validated* without touching any
  disk (`recipe_loader.py`, see [Writing a recipe](#writing-a-recipe)), then
  *applied* (`applier.py`, see [Applying a recipe](#applying-a-recipe)).
- **Applying never mounts anything, and swap is never activated.** The tool
  lays out and formats storage and writes a [report](#the-report). Mounting
  is a separate command reading that report, with the mount points of the
  recipe (`/`, `/boot`, …) placed under a directory chosen at that time
  (`/target`). Swap space can be formatted (`mkswap`) but never activated
  (`swapon`).
- **`format: false` leaves the volume untouched**, for example when a
  partition already holds data.

## Concepts

A recipe has three layers, each referencing the previous one by **handle** (a
unique name matching `^[a-zA-Z0-9_-]+$`):

| Layer | Purpose | Implemented in |
|---|---|---|
| `partitioning` | Which devices get which partitions (sizes, flags, type) | `partition_common.py`, `spacer.py` |
| `volumes` | What is built on partitions: raw, RAID, LVM VG/LV | `volume_common.py` |
| `file_systems` | What is formatted on volumes, and its mount point in the installed system | `fs.py` (formatting) |

### Partitioning

Each group of devices shares one list of partition requests, so identical
layouts can be created on several disks (typically RAID members).

```yaml
partitioning:
  - devices: [/dev/loop100, /dev/loop101]
    recipe:
      keep_partitions:        # true: add to free space instead of wiping
      wipe: false             # true: erase everything first, see "Wiping"
      partitions:
        - handle: partitions_boot
          min_size: 20 MiB
          max_size: 200 MiB
          weight: 500
          p_type: normal
          flags: ["raid"]
```

- **Sizes** accept bytes or a unit (`B, kB, MB … KiB, MiB, GiB …`).
- **Sizing algorithm** (`spacer.py`): every partition gets its `max_size` if
  everything fits. Otherwise each starts at `min_size` and the remaining
  space is shared in proportion to `weight × (max_size − min_size)`, **no
  partition going above its `max_size`**: one that reaches it takes no more,
  and what it leaves is shared again between the others. The whole space is
  used, all sizes are aligned to the device block size, and a `weight` must be
  above 0.
- **Common space**: for a group of devices, the smallest usable space and the
  largest sector size are used so all disks get identical geometry.
- **Reserved areas**: the first 2048 sectors and last 33 sectors are not used.
  With `keep_partitions`, the largest free region is used instead.
- **Wiping** (`wipe: true`, off by default): see below.
- **Partition table**: always `gpt`. With `keep_partitions`, a disk that
  already has a different table type is refused. Only `normal` partitions
  exist (`p_type: normal`).
- **Flags**: any `libparted` flag by name (`raid`, `boot`, `swap`, `esp`, …).

#### Wiping

Old RAID members, LVM physical volumes, file systems and partition tables get
in the way of a new layout. With `wipe: true` (`Recipe(wipe=True)` in Python),
everything is erased as much as possible:

1. **Before the new table is written**, each disk is *released*: whatever is
   built on its partitions is stopped from the top down (RAID arrays with
   `mdadm --stop`, device mapper devices such as LVM volumes with
   `dmsetup remove`, at any depth). Then the signatures of each partition and
   of the disk itself are erased with `wipefs`, which removes the partition
   table.
2. **Once the kernel knows the new partitions**, call
   `HandledPartitions.wipe_partitions()`: it erases the old signatures that the
   previous layout left inside the new partitions.

3. **When the volumes are created**, the RAID arrays and logical volumes built
   (even indirectly) on the partitions of a wiping group are wiped too. Wiping
   the partitions is not enough: when the same layout is created again, the
   data of an array starts at the same place of its partitions, so what the
   previous array held there, such as an LVM physical volume, shows up in the
   new one. A RAID array gets its signatures erased once created, and
   `lvcreate` is told to wipe and zero (`--yes --wipesignatures y --zero y`).
   The `wipe` argument of `RaidVolume` and `LvmLvVolume` does this; the applier
   sets it.

`wipe` can not be combined with `keep_partitions`. A mounted file system is not
unmounted: the device is busy and wiping fails.

### Volumes

| `type` | Class | Notes |
|---|---|---|
| `raw` | `RawVolume` | Uses a single partition as is |
| `raid` | `RaidVolume` | `mdadm --create`; levels 0, 1, 4, 5, 6, 10; spares; metadata `0`, `0.90`, `1`, `1.0`, `1.2` |
| `volume_group` | `LvmVgVolume` | `vgcreate` named after the handle, over partitions and/or other volumes |
| `logical_volume` | `LvmLvVolume` | `lvcreate -l N%VG` on a volume group |

RAID `dev_indices` / `spare_indices` select which of the partitions sharing a
handle (one per disk, in device order) are active members and spares.

### File systems

Formatters live in `fs.py`:

| `type` | Command | Label limit | Notes |
|---|---|---|---|
| `ext2`, `ext3`, `ext4` | `mkfs.ext*` | 16 bytes | block size 1024, 2048 or 4096 |
| `fat16`, `fat32` | `mkfs.fat` | 11 | characters not allowed in a FAT label become `_` |
| `xfs` | `mkfs.xfs -f` | 12 bytes | needs a device of about 300 MB or more |
| `btrfs` | `mkfs.btrfs -f` | 254 bytes | needs a device of about 110 MB or more |
| `exfat` | `mkfs.exfat` | 11 characters | |
| `ntfs` | `mkfs.ntfs -Q -F` | 128 characters | quick format |
| `swap` | `mkswap` | 15 bytes | formatted only, never activated |

Labels may contain spaces, they are cut to the limit of the file system, and
default to the device name. Formatting replaces whatever is on the device:
`xfs`, `btrfs` and `ntfs` are forced over an existing file system.

## Writing a recipe

```yaml
partitioning:                  # see "Partitioning"
  - devices: [/dev/sda, /dev/sdb]
    recipe:
      wipe: false              # optional, see "Wiping"
      keep_partitions: false   # optional
      partitions:
        - {handle: raid_boot, min_size: 512 MiB, max_size: 1 GiB, weight: 10,
           flags: [raid]}       # p_type is optional, always normal
volumes:                       # in the order they are built
  - handle: md_boot
    type: raid
    params:
      partitions_handle: raid_boot
      level: 1
      dev_indices: [0, 1]
      raid_dev_name: /dev/md0
file_systems:
  - type: ext4
    volume_handle: md_boot
    mount: /boot               # mount point in the installed system
```

Volume `params` by `type`:

| `type` | `params` |
|---|---|
| `raw` | `partitions_handle`: partitions of a handle created on one disk only |
| `raid` | `partitions_handle`, `level` (0, 1, 4, 5, 6, 10), `dev_indices`, `raid_dev_name` (`/dev/mdN`); optional `spare_indices`, `meta_data` (`"0"`, `"0.90"`, `"1"`, `"1.0"`, `"1.2"`, default `"1.2"`; quote it: `0.90` is read as a number) |
| `volume_group` | `partitions_handles` and/or `volumes_handles`; the volume group is named after the handle of the volume |
| `logical_volume` | `volume_handle` of a volume group, `vg_percent` (more than 0, at most 100) |

File systems: `type` (see [File systems](#file-systems)), `volume_handle`, and
optionally `format` (default `true`; `false` leaves the volume alone), `label`
(only when formatting), `mount`, `mount_options` (default `defaults`, `sw` for
swap) and `fsck_pass` (0, 1 or 2; by default 1 for `/` then 2 for ext and FAT
file systems, 0 for the others). A swap has no `mount`.

`load_recipe(path)` (or `loads_recipe(text)`) reads a recipe and returns typed
objects; nothing is read from the disks. **Every problem found is reported at
once**, each with its place (`volumes[2].params.level: …`), as a `RecipeError`
with the list in `.errors`. It checks that:

- keys are known and present, and values have the right type;
- sizes, flags and handles are valid, `min_size` is above 0 and not above
  `max_size`, `weight` is at least 1, and `wipe` is not used with
  `keep_partitions`;
- a device is in one group only, and a partitions handle is not used twice;
- volumes only use partitions handles and volumes that exist, **defined
  before them** in the list;
- each partition and each volume is used once: by one volume, or one volume
  group, or one file system (a volume group is shared by its logical
  volumes, whose sizes add up to at most 100%);
- RAID devices and spares fit the level and the number of disks of the
  partitions handle, and RAID device names are unique;
- a file system is on a raw, RAID or logical volume that nothing else uses,
  there is one per volume, mount points are absolute and unique, and swap has
  none.

What depends on the disks, such as having enough space, can only be checked
when the recipe is applied.

## Applying a recipe

```python
from py_part_recipe.applier import apply_recipe
from py_part_recipe.recipe_loader import load_recipe

report = apply_recipe(load_recipe("recipe.yml"))   # needs root or sudo
print(report.dumps("yaml"))
print(report.fstab())
```

The stages run in this order, each one needs the previous:

1. **`partitioning`**: the disks are wiped if asked, partitioned (one table per
   disk), the kernel is told about the new partitions, and the old signatures
   left inside them are wiped if asked.
2. **`volumes`**: raw, RAID and LVM volumes are created in the order of the
   recipe.
3. **`file_systems`**: the volumes that are not `format: false` are formatted.
4. **`report`**: the system is read to describe what was created.

Nothing is written until every group of disks is known to be possible: the
disks must exist and be block devices, and the partitions must fit. The
volumes are all checked against the partitions before the first one is built.

`Applier(recipe)` runs the same stages one at a time (`partition()`,
`build_volumes()`, `format_file_systems()`, `build_report()`) for a caller who
wants to act between them; `run()` does them all.

**When a stage fails, nothing is undone.** `ApplyError` has the `stage`, the
`cause` and `created`, what was already changed (`disks modified`, `volumes
built`, `file systems formatted`). See
[Responsibility and failure handling](#responsibility-and-failure-handling).

## Command line

```bash
uv run py-part-recipe validate recipe.yml
sudo uv run py-part-recipe apply recipe.yml --output report.yml
uv run py-part-recipe fstab report.yml > fstab
sudo uv run py-part-recipe mount report.yml --root /target --bind-system
sudo uv run py-part-recipe umount report.yml --root /target
```

(`python -m py_part_recipe …` does the same.)

| Command | Does |
|---|---|
| `validate RECIPE` | Reads the recipe and lists **every** problem found. Touches no disk, needs no privileges. |
| `apply RECIPE` | Applies the recipe and writes the report. Asks for confirmation after listing the disks it will change, unless `--yes` is given. `--format yaml\|json` and `--output FILE` choose the format (by default from the extension of the file, else yaml) and where it goes (by default the standard output). |
| `fstab REPORT` | Prints the fstab of a report written by `apply`, the file systems named by UUID. A file system without UUID is left out with a warning. |
| `mount REPORT --root DIR` | Mounts the file systems of the report under `DIR` (a mount point `/boot` goes to `DIR/boot`), by UUID, from the shortest mount point to the longest so a parent comes before its children. Directories are created. Swap is not activated. `--bind-system` then binds `/dev`, `/dev/pts`, `/proc`, `/sys` and `/run` of this system under `DIR`, for a chroot. |
| `umount REPORT --root DIR` | Unmounts what `mount` did, the last one mounted first: the bind mounts, then the file systems. |

`mount` refuses `/` as the root, and refuses to start if something is already
mounted at or under it. A file system that must be mounted but has no UUID
stops it **before** the first mount: skipping it could mount its children over
the directories of this system. It stops at the first failure and **unmounts
nothing**; the error lists what is mounted.

`umount` does the opposite: it tries **every** mount point even when one fails,
because leaving the bind mounts of `/dev` in place is the worst outcome. What
is not mounted is skipped, so it can be run twice. Nothing is forced or
detached lazily: a busy mount point stays mounted and is reported, with the
list of what is still mounted. Do not delete the directory (`rm -r /target`)
while the bind mounts are in place: it would delete the files of `/dev`.

What a command produces goes to the standard output (the report, the fstab);
questions, summaries, errors and warnings go to the standard error, so
`apply recipe.yml --yes > report.yml` and `fstab report.yml > fstab` work as
expected. `apply` checks that the report can be written *before* changing any
disk, and if writing it fails afterwards it prints it on the standard output
instead of losing it.

Exit codes, so that a script can tell what happened:

| Code | Meaning |
|---|---|
| 0 | done |
| 1 | the recipe or report is not valid, or a file cannot be read or written |
| 2 | applying failed: **nothing is undone**, the message says the stage and what was already changed |
| 3 | not applied, the confirmation was not given |
| 4 | mounting or unmounting failed: what is mounted, or still mounted, is listed |
| 64 | wrong command line |
| 130 | interrupted while applying: the state of the disks is unknown |

## The report

Once a recipe is applied, the system itself is read to describe what exists:
`build_report(partitions, volumes, file_systems)` in `report_builder.py`
returns a `Report` (`report.py`). Nothing is copied from what was asked, so the
report tells the truth: a label cut to the limit of its file system is
reported as stored.

```yaml
version: 1
partitions:                       # by handle, one per disk
  partitions_raid_md1:
    - {device: /dev/loop100p2, disk: /dev/loop100, number: 2,
       partuuid: 6f1c0a52-…, size_bytes: 104857600}
volumes:                          # by handle
  volume_md1: {type: raid, device: /dev/md31, uuid: 5d0e1a2b:…}
  volume_lv_root:
    {type: logical_volume, device: /dev/vg/lv_root, uuid: Tf3xAb-…}
file_systems:
  - volume: volume_lv_root
    type: ext4
    formatted: true               # false: the recipe left the volume alone
    device: /dev/vg/lv_root
    uuid: 3f2a9c1e-…
    label: root
    mount: /                      # mount point in the installed system
    mount_options: defaults
    fsck_pass: 1
```

- UUIDs come from `blkid` (file systems and partitions), `mdadm` (RAID arrays)
  and the lvm tools (volume groups and logical volumes). A file system that
  was not formatted by the recipe and holds nothing has no UUID.
- A file system that was formatted but is not the expected one is an error.
- The report is written as yaml or json (`dumps`, `dump`) and read back from
  either (`loads`, `load`), with all the problems of an invalid report listed.
- `fstab()` renders the file systems by `UUID=`, parents before children and
  swap last. A file system with no UUID is left out with a warning.
- Mount points are the paths in the installed system (`/`, `/boot`, …); the
  directory where they get mounted is chosen when mounting.

## Responsibility and failure handling

Applying a recipe is **destructive and not transactional**. The tool does not
undo anything:

- If a stage fails (partitioning, RAID, LVM, formatting), it stops and reports
  the error. Everything created by the earlier stages stays in place:
  partition tables, RAID arrays, volume groups, file systems.
- Cleaning up, or deciding to re-run on a partly built layout, is up to the
  caller.
- Nothing checks whether a disk is mounted or holds data you care about.

So:

- **Command line users** must check the recipe and the target devices before
  applying it, and be ready to clean up after a failure.
- **Developers using the library** must handle exceptions from each stage and
  decide what to do with the partial result. The tool does not make that
  decision for them.

## Requirements

- **Linux** and **Python 3.10 or newer** (no upper limit). The tests that need
  no privileges were run on 3.10, 3.12 and 3.13.
- **Root, or passwordless `sudo`** for the commands below: when not run as
  root, external commands are prefixed with `sudo`.
- **[uv](https://docs.astral.sh/uv/)** to manage the environment.
- **To build `pyparted`**, which is compiled at install time (it has no
  wheels): a C compiler, the Python headers and the `libparted` headers.
- **The command line tools of the features you use**:

| Feature | Commands | Debian/Ubuntu package |
|---|---|---|
| Partitions (runtime) | `libparted` | `libparted2` |
| Partitions (build) | `libparted` headers | `libparted-dev` |
| Kernel notification | `partx` | `util-linux` |
| RAID | `mdadm` | `mdadm` |
| LVM | `pvs`, `pvcreate`, `pvdisplay`, `vgcreate`, `vgdisplay`, `lvcreate` | `lvm2` |
| ext2/3/4 | `mkfs.ext2`, `mkfs.ext3`, `mkfs.ext4` | `e2fsprogs` |
| FAT | `mkfs.fat` | `dosfstools` |
| xfs | `mkfs.xfs` | `xfsprogs` |
| btrfs | `mkfs.btrfs` | `btrfs-progs` |
| exFAT | `mkfs.exfat` | `exfatprogs` |
| NTFS | `mkfs.ntfs` | `ntfs-3g` |
| swap | `mkswap` | `util-linux` |

Debian/Ubuntu, everything:

```bash
sudo apt-get install -y build-essential python3-dev libparted-dev libparted2 \
    util-linux mdadm lvm2 e2fsprogs dosfstools xfsprogs btrfs-progs \
    exfatprogs ntfs-3g
```

Package names for other distributions are not documented yet: use the
commands column to find them. Nothing checks that the commands are installed
before they are run, a missing one is reported when it is first needed.

## Installation

### On the system that partitions: pip, no uv needed

The package is a normal wheel, `py_part_recipe-<version>-py3-none-any.whl`: pure
Python, any Python 3.10 or newer. It installs the `py-part-recipe` command, the
`py_part_recipe` module, and the example recipes in
`<prefix>/share/py-part-recipe/examples/`.

```bash
python3 -m venv /opt/py-part-recipe        # Debian/Ubuntu: apt install python3-venv
/opt/py-part-recipe/bin/pip install -U pip wheel
/opt/py-part-recipe/bin/pip install py_part_recipe-0.1.0-py3-none-any.whl
sudo /opt/py-part-recipe/bin/py-part-recipe --help
```

Use a virtual environment (or `pipx`): recent Debian and Ubuntu refuse `pip
install` in the system Python (PEP 668). Run the command with its full path,
`root` does not have the environment's `bin` in its `PATH`.

`pyparted` has no wheel on PyPI: `pip` **compiles it** when it installs. That
needs, on that system, a C compiler, the Python headers and the `libparted`
headers (`apt-get install gcc python3-dev libparted-dev`), the tools of the
[requirements](#requirements), and access to PyPI.

#### Without a compiler or network: build the wheels once

On a machine of the same architecture and the same Python minor version (3.10,
3.12...) as the target, with the build requirements above, collect every wheel
the package needs:

```bash
python3 -m venv build && build/bin/pip install -U pip wheel
build/bin/pip wheel py_part_recipe-0.1.0-py3-none-any.whl -w wheelhouse
```

`wheelhouse/` now holds our wheel, `PyYAML` and a compiled `pyparted`
(`...cp310-linux_x86_64.whl`). Copy it to the target and install from it only:

```bash
python3 -m venv /opt/py-part-recipe
/opt/py-part-recipe/bin/pip install --no-index --only-binary :all: \
    --find-links wheelhouse py-part-recipe
```

The target only needs `libparted` at run time (`libparted2`, `libparted2t64`
on Ubuntu 24.04) and the tools of the [requirements](#requirements). The
compiled wheel is tied to the Python version and the CPU architecture it was
built for.

The `python3-parted` package of Debian and Ubuntu can replace the compiled
wheel, but it is older (3.12.0 on Ubuntu 24.04) than the 3.13.0 this package is
developed and tested with, so that route is not tested.

#### Building the wheel

On the development machine (this is where `uv` is used, not on the target):

```bash
uv build            # or: python -m build
```

writes `dist/py_part_recipe-<version>.tar.gz` and the wheel. The version is the
`version` line of `pyproject.toml`. Nothing is published anywhere.

### Development environment: uv

```bash
uv sync
```

This creates `.venv` with the dependencies (`pyparted`, `pyyaml`) and the
development tools (`pytest`, `black`, `ruff`). Dependencies are locked in
`uv.lock`.

## Usage (Python API)

```python
from py_part_recipe.partition_common import (
    PartitionRequest, Recipe, Partitionner, HandledPartitions,
)
from py_part_recipe.volume_common import HandledVolumes, RaidVolume

reqs = [PartitionRequest(handle="parts_md0", min_size="20 MiB",
                         max_size="200 MiB", weight=100, flags=["raid"])]
recipe = Recipe(devices_names=["/dev/loop100", "/dev/loop101"],
                part_requests=reqs)

parts = HandledPartitions([Partitionner(recipe)])
parts.create_partitions_mapping()   # compute and stage in memory
print(parts)                        # review the planned layout
parts.commit()                      # write tables + notify the kernel

md0 = RaidVolume(partitionners=parts, partitions_handle="parts_md0",
                 handle="volume_md0", raid_dev_name="/dev/md30",
                 level=1, dev_indices=[0, 1])
md0.build()
```

## Development

### Test environment

The tests operate on real loop devices, so they need root privileges and
system packages. `pretest.sh` sets this up on Debian/Ubuntu: it installs
packages, creates the loop devices (sparse files in `/tmp`, grown if they
already exist), allows the needed commands in sudoers and runs `uv sync` (uv
must be installed, and available in the `PATH` of whoever runs the script).

- `/dev/loop100`…`/dev/loop106` are 512 MB: enough to apply
  `examples/example_recipe.yml` (which uses exactly these seven disks) and to
  format `xfs` and `btrfs`.
- `/dev/loop120` and `/dev/loop121` are 40 MB. The tests of the partition
  sizes assert exact geometries of such disks.

Applying the example by hand on the test devices works, but the tests wipe
`loop100`–`102` and need the md and volume group names of the tests: undo the
example (`umount`, then `wipe_disk` on each disk) before running them.

```bash
bash pretest.sh
```

Then:

```bash
uv run pytest
```

The tests destroy the devices they use, so they are guarded
(`tests/device_guard.py`, `tests/conftest.py`):

- They refuse to start if `/dev/loop100`–`106` and `120`–`121` are not backed
  by `/tmp/loopN`, or if an md array `md1NN` or a volume group `vg_test*` already
  exists, because the tests clean those up by name.
- Every program the tests run, every `parted` device and every write open of a
  `/dev` file is checked first. Only `/dev/loop100`–`106` and `120`–`121` (and
  their partitions), `/dev/md100`–`199` and `vg_test*` volume groups are accepted;
  anything else raises an error before anything happens. The real commands
  still run, only their arguments are checked.

- Mounting is checked too: `mount`, `umount` and `mkdir` may only work inside
  the temporary directory, a bind mount can only bind a directory of the
  temporary directory (a bind of `/dev` left behind would be walked into when
  the temporary directory is cleaned up, so the tests bind fake directories),
  a device mounted by UUID must be a test device, and `mount` without a source
  and a target (an fstab entry), `-a`, `umount -a/-l/-f` are refused.

When writing a test that needs another device name, extend the allowed names
in `tests/device_guard.py` on purpose.

Tests cover the sizing algorithm, device detection, partitioning (flush and
keep modes), and RAID/LVM volume creation. Lint/format with `ruff` and `black`.

### Making a release

A release is made by a tag. The workflow `.github/workflows/release.yml` runs
when a tag `vX.Y.Z` is pushed:

```bash
# 1. set the version in pyproject.toml, run the tests (they need loop devices
#    and root, the workflow does not run them), commit
# 2. tag that commit and push the tag
git tag -a v0.2.0 -m "py-part-recipe 0.2.0"
git push origin main v0.2.0
```

The workflow then:

1. **checks that the tag is the version of `pyproject.toml`** (`v0.2.0` for
   `0.2.0`) and stops if not, so a tag can not publish a wheel of another
   version;
2. builds the wheel and the sdist, checks them with `twine`, installs the wheel
   in a clean virtual environment and runs the command and the example recipe
   with it;
3. builds, for Python 3.10, 3.11, 3.12 and 3.13 on Linux x86_64, the folder of
   wheels needed to [install without a compiler or network](#without-a-compiler-or-network-build-the-wheels-once)
   and checks that it installs with no index;
4. creates the GitHub release with all of these, a `SHA256SUMS` file, and
   generated release notes. A tag with a suffix (`v0.2.0rc1`, `v0.2.0-beta.1`)
   makes a pre-release.

If pushing the tag does not start the workflow, run it by hand: on GitHub,
Actions, Release, Run workflow, and choose the **tag** (not a branch) in "Use
workflow from". From a branch it stops at the first check.

If a step fails nothing is published; delete the tag (`git push origin :v0.2.0`,
`git tag -d v0.2.0`), fix, and tag again. The workflow's own checks of a tag
can be run before pushing it: `python .github/scripts/check_version.py v0.2.0`.

## Project status

Done:
- Weighted partition sizing with block alignment
- GPT partitioning on multiple identical disks, optional keep-existing mode
- Raw, RAID and LVM (VG/LV) volumes
- ext2/3/4, FAT16/32, xfs, btrfs, exFAT, NTFS and swap (`mkswap`) formatting
- Optional wiping of disks and partitions
- Reading and validating a recipe file (`recipe_loader.py`)
- Applying a recipe, stage by stage, with a report of what exists
  (`applier.py`)
- The command line: `validate`, `apply`, `fstab`, `mount` and `umount`
  (`cli.py`, mounting in `mounting.py`)
- The report (yaml/json, validation, fstab rendering) built from the system:
  partitions, RAID/LVM volumes and file systems with their UUIDs

Not yet done (see `examples/example_recipe.yml` for the intended format):
- Other file systems (`f2fs`, `zfs`, HFS, APFS, …)
- Dry run, teardown, and resize operations

Out of scope by design: activating swap, mounting while a recipe is applied
(it is the separate `mount` command), and rolling back a failed run (see
below).

## License

MIT, see `LICENSE`.
