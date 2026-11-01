# py_part_recipe

Declarative disk layout for Linux. You describe partitions, volumes (RAID, LVM)
and file systems in a YAML *recipe*, and the library computes the layout and
applies it with `libparted`, `mdadm`, LVM tools and `mkfs.*`.

> **Status: early development (v0.1.0).** The building blocks (partition sizing,
> partitioning, RAID, LVM, ext/FAT formatting) exist and are tested. There is
> **no CLI and no YAML loader yet**: the recipe in `examples/example_recipe.yml`
> shows the target format, but today you drive the Python classes directly.
> See [Project status](#project-status).

> **Warning:** this software repartitions and formats block devices. Wrong
> input destroys data. Use it on disposable disks or loop devices until it
> matures.

## Concepts

A recipe has three layers, each referencing the previous one by **handle** (a
unique name matching `^[a-zA-Z0-9_-]+$`):

| Layer | Purpose | Implemented in |
|---|---|---|
| `partitioning` | Which devices get which partitions (sizes, flags, type) | `partition_common.py`, `spacer.py` |
| `volumes` | What is built on partitions: raw, RAID, LVM VG/LV | `volume_common.py` |
| `file_systems` | What is formatted on volumes, and where it mounts | `fs.py` (formatting only) |

### Partitioning

Each group of devices shares one list of partition requests, so identical
layouts can be created on several disks (typically RAID members).

```yaml
partitioning:
  - devices: [/dev/loop100, /dev/loop101]
    recipe:
      keep_partitions:        # true: add to free space instead of wiping
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
- **Partition table**: `gpt` (default) or `msdos`.
- **Flags**: any `libparted` flag by name (`raid`, `boot`, `swap`, `esp`, …).

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

Implemented formatters (`fs.py`): `ext2`, `ext3`, `ext4` (`mkfs.ext*`, block
size 1024/2048/4096, label truncated to 16 chars) and `fat16`, `fat32`
(`mkfs.fat`, label sanitised to 11 chars).

## Requirements

- Linux, Python ≥ 3.9 (the code uses `X | None` annotations, so effectively
  ≥ 3.10).
- Root, or passwordless `sudo`: external commands are prefixed with `sudo`
  when not run as root.
- `libparted` and its headers (to build `pyparted`), plus the tools for the
  features you use: `mdadm`, `lvm2`, `e2fsprogs`, `dosfstools`.

Debian/Ubuntu:

```bash
sudo apt-get install -y build-essential libparted-dev mdadm lvm2 \
    e2fsprogs dosfstools python3-dev
```

## Installation

```bash
poetry install
```

`pyparted` is pulled from its GitHub release tarball (v3.13.0) and compiled
locally.

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

parts = HandledPartitions([Partitionner(recipe, table_type="gpt")])
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

The tests operate on real loop devices (`/dev/loop100`…), so they need root
privileges and system packages. `pretest.sh` sets this up on Debian/Ubuntu: it
installs packages, creates the loop devices, checks passwordless sudo and
builds the Poetry environment.

```bash
bash pretest.sh
```

Then:

```bash
poetry run pytest
```

Tests cover the sizing algorithm, device detection, partitioning (flush and
keep modes), and RAID/LVM volume creation. Lint/format with `ruff` and `black`.

## Project status

Done:
- Weighted partition sizing with block alignment
- GPT/msdos partitioning on multiple identical disks, optional keep-existing mode
- Raw, RAID and LVM (VG/LV) volumes
- ext2/3/4 and FAT16/32 formatting

Not yet done (see `examples/example_recipe.yml` for the intended format):
- YAML recipe parsing/validation and a CLI entry point
- Mounting, swap, and other file systems (`xfs`, `btrfs`, …) listed in `pretest.sh`
- Dry-run/rollback, teardown, and resize operations

## License

MIT, see `LICENSE`.
