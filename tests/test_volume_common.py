import json
import os
import subprocess
from typing import List
import pytest
from py_part_recipe.partition_common import (
    HandledPartitions,
    PartitionRequest,
    Partitionner,
    Recipe,
)
from parted import Partition
from device_guard import ensure_vg_absent
from device_helpers import make_parts

from py_part_recipe.volume_common import (
    HandledVolumes,
    LvmLvVolume,
    LvmVgVolume,
    RaidVolume,
    RawVolume,
)


def setup_raid_disk(disks: List[str]) -> Partitionner:
    partitions = [
        PartitionRequest("efi_part", "2MiB", "5MiB", 100, flags=["raid", "esp"]),
        PartitionRequest("boot_part", "4MiB", "10MiB", 100, flags=["raid"]),
        PartitionRequest("root_part", "10MiB", "20MiB", 100, flags=["raid"]),
        PartitionRequest("var_part", "5MiB", "5MiB", 100, flags=["raid"]),
    ]
    main_recipe = Recipe(disks, partitions)
    main_parter = Partitionner(main_recipe)
    return main_parter


def clean_raid(
    handled_parts: HandledPartitions,
    handles: List[str],
):
    for index, handle in enumerate(handles):
        subprocess.run(["sudo", "mdadm", "--stop", "--force", f"/dev/md{100+index}"])
        parts: List[Partition] = list(
            handled_parts.partitionners[0].created_parttions_by_handle[handle]
        )
        parts_paths = [part.path for part in parts]
        subprocess.run(["sudo", "mdadm", "--zero-superblock"] + parts_paths)
        if os.path.exists(f"/dev/md{100+index}"):
            subprocess.run(
                ["sudo", "mdadm", "--stop", "--force", f"/dev/md{100+index}"]
            )


@pytest.mark.parametrize(
    "disks_paths,dev_indices,spare_indices,level,handles",
    (
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102"],
            [0, 1],
            [2],
            1,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102", "/dev/loop103"],
            [0, 1, 2],
            [3],
            5,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
        (
            ["/dev/loop100", "/dev/loop101", "/dev/loop102"],
            [0, 1, 2],
            [],
            5,
            ["efi_part", "boot_part", "root_part", "var_part"],
        ),
    ),
)
def test_good_raid_volume(
    disks_paths,
    dev_indices,
    spare_indices,
    level,
    handles,
):
    handled_parts = HandledPartitions([setup_raid_disk(disks_paths)])
    make_parts(handled_parts)
    try:
        for index, handle in enumerate(handles):
            raid_volume = RaidVolume(
                raid_dev_name=f"/dev/md{100+index}",
                level=level,
                dev_indices=dev_indices,
                spare_indices=spare_indices,
                partitionners=handled_parts,
                partitions_handle=handle,
                handle=f"md{100+index}_{handle}",
            )
            assert not raid_volume.built
            raid_volume.build()
            assert raid_volume.built
    except:
        clean_raid(handled_parts, handles)
        raise
    clean_raid(handled_parts, handles)


def setup_lvm_disk(disks: List[str]) -> Partitionner:
    partitions = [
        PartitionRequest("part_1", "10MiB", "100MiB", 100, flags=["lvm"]),
        PartitionRequest("part_2", "10MiB", "100MiB", 100, flags=["lvm"]),
    ]
    main_recipe = Recipe(disks, partitions)
    main_parter = Partitionner(main_recipe)
    return main_parter


def setup_raid_disk_2(disks: List[str]) -> Partitionner:
    partitions = [
        PartitionRequest("root_part", "10MiB", "200MiB", 100, flags=["raid"]),
    ]
    main_recipe = Recipe(disks, partitions)
    main_parter = Partitionner(main_recipe)
    return main_parter


@pytest.mark.parametrize(
    "native_disks,handles,raid_disk,raid_handles,vg_name,lvname",
    (
        (
            ["/dev/loop100", "/dev/loop101"],
            ["part_1", "part_2"],
            ["/dev/loop102", "/dev/loop103"],
            ["root_part"],
            "vg_test",
            "lv_test",
        ),
    ),
)
def test_good_lvm_volume(
    native_disks, handles, raid_disk, raid_handles, vg_name, lvname
):
    # the volume group is removed by name at the end of the test
    ensure_vg_absent(vg_name)
    native_parter = setup_lvm_disk(native_disks)
    raid_parter = setup_raid_disk_2(raid_disk)
    handled_parts = HandledPartitions([raid_parter, native_parter])
    make_parts(handled_parts)
    handled_volumes = HandledVolumes()
    try:
        for index, handle in enumerate(raid_handles):
            raid_volume = RaidVolume(
                raid_dev_name=f"/dev/md{100+index}",
                level=1,
                dev_indices=[0, 1],
                spare_indices=None,
                partitionners=handled_parts,
                partitions_handle=handle,
                handle=f"md{100+index}_{handle}",
            )
            assert not raid_volume.built
            raid_volume.build()
            assert raid_volume.built
            handled_volumes.add(raid_volume)
    except:
        clean_raid(handled_parts, raid_handles)
        raise
    vg_volume = LvmVgVolume(
        handled_parts=handled_parts,
        partitions_handles=handles,
        handled_vols=handled_volumes,
        volumes_handles=[
            f"md{100+index}_{handle}" for index, handle in enumerate(raid_handles)
        ],
        handle=vg_name,
    )
    handled_volumes.add(vg_volume)
    lv_volume = LvmLvVolume(
        handled_vols=handled_volumes,
        volume_handle=vg_volume.handle,
        handle=lvname,
        vg_percent=80,
    )
    handled_volumes.add(lv_volume)
    handled_volumes.build()
    assert handled_volumes.built
    assert handled_volumes.handle_to_dev[lvname] == f"/dev/{vg_name}/{lvname}"
    vgcreated_cmd = subprocess.run(
        ["sudo", "vgs", "--report-format", "json"], capture_output=True
    )
    vgcreated_data = json.loads(vgcreated_cmd.stdout.decode("utf-8"))
    vgreated = vg_volume.handle in [
        vg["vg_name"] for vg in vgcreated_data["report"][0]["vg"]
    ]
    lvs_cmd = subprocess.run(
        ["sudo", "lvs", "--reportformat", "json"], capture_output=True
    )
    lvs_data = json.loads(lvs_cmd.stdout.decode("utf-8"))

    try:
        assert vgreated and vgcreated_cmd.returncode == 0
        assert lvname in [lv["lv_name"] for lv in lvs_data["report"][0]["lv"]]
    except:
        raise
    finally:
        if lvname in [lv["lv_name"] for lv in lvs_data["report"][0]["lv"]]:
            subprocess.run(["sudo", "lvremove", "-f", lvname], capture_output=True)

        pvs_cmd = subprocess.run(
            ["sudo", "pvs", "--reportformat", "json"], capture_output=True
        )
        pvs_data = json.loads(pvs_cmd.stdout.decode("utf-8"))
        pv_to_detelete = [
            pv["pv_name"]
            for pv in pvs_data["report"][0]["pv"]
            if pv["vg_name"] == vg_name
        ]
        subprocess.run(["sudo", "vgremove", "-f", vg_name], capture_output=True)
        for pv in pv_to_detelete:
            subprocess.run(["sudo", "pvremove", "-f", pv], capture_output=True)
        clean_raid(handled_parts, raid_handles)


def test_raw_volume():
    handled_parts = HandledPartitions([setup_raid_disk(["/dev/loop100"])])
    make_parts(handled_parts)
    raw_volume = RawVolume(
        partitionners=handled_parts, partitions_handle="efi_part", handle="raw_efi"
    )
    partition = handled_parts.get_partitions_by_handle("efi_part")[0]
    assert raw_volume.built
    assert raw_volume.handle == "raw_efi"
    assert raw_volume.sys_device == partition.path


def test_raw_volume_needs_exactly_one_partition():
    handled_parts = HandledPartitions(
        [setup_raid_disk(["/dev/loop100", "/dev/loop101"])]
    )
    make_parts(handled_parts)
    with pytest.raises(ValueError, match="efi_part"):
        RawVolume(
            partitionners=handled_parts, partitions_handle="efi_part", handle="raw_efi"
        )


def test_raw_volume_partitions_not_committed():
    handled_parts = HandledPartitions([setup_raid_disk(["/dev/loop100"])])
    with pytest.raises(ValueError, match="not yet set"):
        RawVolume(
            partitionners=handled_parts, partitions_handle="efi_part", handle="raw_efi"
        )


def raid_kwargs(**overrides):
    kwargs = dict(
        raid_dev_name="/dev/md100",
        level=1,
        dev_indices=[0, 1],
        spare_indices=None,
        partitionners=HandledPartitions([setup_raid_disk(["/dev/loop100"])]),
        partitions_handle="efi_part",
        handle="md100_efi",
    )
    kwargs.update(overrides)
    return kwargs


@pytest.mark.parametrize("handle", ("", None, ["efi_part"]))
def test_raid_volume_bad_partitions_handle(handle):
    with pytest.raises(ValueError, match="partitions_handle"):
        RaidVolume(**raid_kwargs(partitions_handle=handle))


def test_raid_volume_bad_metadata():
    with pytest.raises(ValueError, match="meta_data version 9"):
        RaidVolume(**raid_kwargs(meta_data="9"))


def test_raid_volume_bad_level():
    with pytest.raises(ValueError, match="raid level 3"):
        RaidVolume(**raid_kwargs(level=3))


def test_raid_volume_partitions_count_mismatch():
    # nothing was partitioned, no partition matches the handle
    with pytest.raises(ValueError, match="You expect 2 devices.*0 partitions"):
        RaidVolume(**raid_kwargs())


def test_handled_volumes_duplicate_handle():
    handled_parts = HandledPartitions([setup_raid_disk(["/dev/loop100"])])
    make_parts(handled_parts)
    handled_volumes = HandledVolumes()
    handled_volumes.add(
        RawVolume(
            partitionners=handled_parts, partitions_handle="efi_part", handle="raw_dup"
        )
    )
    with pytest.raises(KeyError, match="raw_dup"):
        handled_volumes.add(
            RawVolume(
                partitionners=handled_parts,
                partitions_handle="boot_part",
                handle="raw_dup",
            )
        )


def test_handled_volumes_build_unregistered_dependency():
    # the dependency check runs before any system command
    handled_parts = HandledPartitions([setup_raid_disk(["/dev/loop100"])])
    handled_volumes = HandledVolumes()
    handled_volumes.add(
        LvmVgVolume(
            handled_parts=handled_parts,
            partitions_handles=[],
            handled_vols=handled_volumes,
            volumes_handles=["vol_missing"],
            handle="vg_unregistered",
        )
    )
    with pytest.raises(LookupError, match="vol_missing.*not registered"):
        handled_volumes.build()


def test_handled_volumes_build_wrong_order():
    # the lv is registered before its vg, nothing must be created
    handled_parts = HandledPartitions([setup_raid_disk(["/dev/loop100"])])
    handled_volumes = HandledVolumes()
    handled_volumes.add(
        LvmLvVolume(
            handled_vols=handled_volumes,
            volume_handle="vg_late",
            handle="lv_early",
            vg_percent=50,
        )
    )
    handled_volumes.add(
        LvmVgVolume(
            handled_parts=handled_parts,
            partitions_handles=[],
            handled_vols=handled_volumes,
            volumes_handles=[],
            handle="vg_late",
        )
    )
    with pytest.raises(LookupError, match="vg_late.*not built yet"):
        handled_volumes.build()
    assert not handled_volumes.get_by_handle("vg_late").built
