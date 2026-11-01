"""Recipes and reports shared by the tests."""

import yaml

from py_part_recipe.report import (
    FileSystemReport,
    PartitionReport,
    Report,
    VolumeReport,
)

MIB = 1024 * 1024

DISKS = ["/dev/loop100", "/dev/loop101", "/dev/loop102"]

# The raid is on the two first disks, the other partitions on the third.
# The names fit the test guard: /dev/md100 and the volume group vg_test.
RECIPE = yaml.safe_load("""
partitioning:
  - devices: [/dev/loop100, /dev/loop101]
    recipe:
      wipe: true
      partitions:
        - {handle: raid_part, min_size: 20 MiB, max_size: 20 MiB, weight: 1,
           flags: [raid]}
  - devices: [/dev/loop102]
    recipe:
      wipe: true
      partitions:
        - {handle: boot_part, min_size: 10 MiB, max_size: 10 MiB, weight: 1}
        - {handle: swap_part, min_size: 8 MiB, max_size: 8 MiB, weight: 1}
volumes:
  - handle: md_vol
    type: raid
    params:
      partitions_handle: raid_part
      level: 1
      dev_indices: [0, 1]
      raid_dev_name: /dev/md100
  - {handle: vg_test, type: volume_group, params: {volumes_handles: [md_vol]}}
  - handle: lv_root
    type: logical_volume
    params: {volume_handle: vg_test, vg_percent: 50}
  - handle: lv_home
    type: logical_volume
    params: {volume_handle: vg_test, vg_percent: 25}
  - {handle: boot_vol, type: raw, params: {partitions_handle: boot_part}}
  - {handle: swap_vol, type: raw, params: {partitions_handle: swap_part}}
file_systems:
  - {type: ext4, volume_handle: lv_root, mount: /, label: root}
  - {type: ext4, volume_handle: lv_home, mount: /home, mount_options: noatime}
  - {type: ext2, volume_handle: boot_vol, mount: /boot}
  - {type: swap, volume_handle: swap_vol}
""")


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
