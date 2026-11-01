# py_part_recipe

Declarative disk layout for Linux. You describe partitions, volumes (RAID, LVM)
and file systems in a YAML *recipe*, and the library computes the layout and
applies it with `libparted`, `mdadm`, LVM tools and `mkfs.*`.

> **Status: early development (v0.1.0).** The building blocks (partition sizing,
> partitioning, wiping, RAID, LVM, formatting, the report) exist and are tested,
> and a recipe file can be read and validated (`load_recipe`). There is **no
> CLI and no applier yet**: the recipe in `examples/example_recipe.yml` shows the
> format, but applying it still means driving the Python classes directly.
> See [Project status](#project-status).

> **Warning:** this software repartitions and formats block devices. Wrong
> input destroys data. Use it on disposable disks or loop devices until it
> matures. See [Responsibility and failure handling](#responsibility-and-failure-handling).

## Design choices

- **GPT only.** This is a modern tool: MBR/msdos tables, and the extended and
  logical partitions that go with them, are not supported.
- **Two steps.** A recipe is first *parsed and validated* without touching any
  disk (`recipe_loader.py`, see [Writing a recipe](#writing-a-recipe)), then
  *applied* (not written yet, see [Project status](#project-status)).
- **Applying never mounts anything, and swap is never activated.** The tool
  lays out and formats storage and writes a [report](#the-report). Mounting
  will be a separate command reading that report, with the mount points of the
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
  space is shared in proportion to `weight × (max_size − min_size)`. All sizes
  are aligned to the device block size.
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

Volumes built on freshly wiped partitions start clean, so they need no wipe of
their own. `wipe` can not be combined with `keep_partitions`. A mounted file
system is not unmounted: the device is busy and wiping fails.

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

The tests operate on real loop devices (`/dev/loop100`…`/dev/loop106`), so
they need root privileges and system packages. `pretest.sh` sets this up on
Debian/Ubuntu: it installs packages, creates the loop devices (sparse files in
`/tmp`; `/dev/loop106` is 512 MB so `xfs` and `btrfs` can be formatted, and
it is grown if it already exists), allows the needed commands in sudoers and
runs `uv sync` (uv must be installed, and available in the `PATH` of whoever
runs the script).

```bash
bash pretest.sh
```

Then:

```bash
uv run pytest
```

The tests destroy the devices they use, so they are guarded
(`tests/device_guard.py`, `tests/conftest.py`):

- They refuse to start if `/dev/loop100`–`106` are not backed by
  `/tmp/loopN`, or if an md array `md1NN` or a volume group `vg_test*` already
  exists, because the tests clean those up by name.
- Every program the tests run, every `parted` device and every write open of a
  `/dev` file is checked first. Only `/dev/loop100`–`106` (and their
  partitions), `/dev/md100`–`199` and `vg_test*` volume groups are accepted;
  anything else raises an error before anything happens. The real commands
  still run, only their arguments are checked.

When writing a test that needs another device name, extend the allowed names
in `tests/device_guard.py` on purpose.

Tests cover the sizing algorithm, device detection, partitioning (flush and
keep modes), and RAID/LVM volume creation. Lint/format with `ruff` and `black`.

## Project status

Done:
- Weighted partition sizing with block alignment
- GPT partitioning on multiple identical disks, optional keep-existing mode
- Raw, RAID and LVM (VG/LV) volumes
- ext2/3/4, FAT16/32, xfs, btrfs, exFAT, NTFS and swap (`mkswap`) formatting
- Optional wiping of disks and partitions
- Reading and validating a recipe file (`recipe_loader.py`)
- The report (yaml/json, validation, fstab rendering) built from the system:
  partitions, RAID/LVM volumes and file systems with their UUIDs

Not yet done (see `examples/example_recipe.yml` for the intended format):
- Other file systems (`f2fs`, `zfs`, HFS, APFS, …)
- Dry run, teardown, and resize operations
- Applying a recipe: running the stages in order and writing the report
- The command line: `apply`, `mount`, `umount` and `fstab`

Out of scope by design: activating swap, mounting while a recipe is applied
(it is the separate `mount` command), and rolling back a failed run (see
below).

## License

MIT, see `LICENSE`.
