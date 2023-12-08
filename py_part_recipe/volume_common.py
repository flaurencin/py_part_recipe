from abc import ABC, abstractmethod
import os
import subprocess
from typing import Dict, List, Tuple, Union
import parted
from py_part_recipe.partition_common import HandledPartitions
from py_part_recipe.common import gen_cmd_for_subprocess, validate_handle


class Volume(ABC):
    @abstractmethod
    def build(self):
        ...

    @property
    @abstractmethod
    def built(self) -> bool:
        ...

    @property
    @abstractmethod
    def sys_device(self) -> str:
        ...

    @property
    @abstractmethod
    def handle(self) -> str:
        ...


class HandledVolumes:
    def __init__(self, volumes: Union[Dict[str, Volume], None] = None):
        self.volumes = volumes if volumes else {}
        self.created = False

    def _add_volume(self, volume: Volume):
        if volume.handle in self.volumes:
            raise KeyError(
                f" Volume handle must be unique but {volume.handle}"
                " is already in use"
            )
        self.volumes[volume.handle] = volume

    @property
    def handle_to_dev(self) -> Dict[str, str]:
        return {handle: vol.sys_device for handle, vol in self.volumes.items()}

    def build(self):
        for volume in self.volumes.values():
            volume.build()
        self.created = True

    @property
    def built(self):
        return all([volume.built for volume in self.volumes.values()])

    def get_by_handle(self, handle: str) -> Volume:
        return self.volumes[handle]

    def get_by_handles(self, handles: List[str]):
        return [self.get_by_handle(handle) for handle in handles]


class PartitionBasedVolume(Volume, ABC):
    def __init__(
        self,
        *args,
        partitionners: HandledPartitions,
        partitions_handle: str,
        handle: str,
    ) -> None:
        self.partitions_handle = partitions_handle
        self.partitionners = partitionners
        self._handle = validate_handle(handle)

    @property
    def handle(self) -> str:
        return self._handle


class VolumeBasedVolume(Volume, ABC):
    def __init__(
        self,
        *args,
        handled_vols: HandledVolumes,
        volume_handle: str,
        handle: str,
    ) -> None:
        self._handle = validate_handle(handle)
        self.handled_vols = handled_vols
        self.volume_handle = volume_handle

    @property
    def handle(self) -> str:
        return self._handle

    def get_volume_by_handle(self) -> Volume:
        return self.handled_vols.get_by_handle(self.volume_handle)


class MulitMixedVolume(Volume, ABC):
    def __init__(
        self,
        *args,
        handled_parts: HandledPartitions,
        partitions_handles: List[str],
        handled_vols: HandledVolumes,
        volumes_handles: List[str],
        handle: str,
    ) -> None:
        self._handle = validate_handle(handle)
        self.handled_vols = handled_vols
        self.handled_parts = handled_parts
        self.volumes_handles = [validate_handle(handle) for handle in volumes_handles]
        self.partitions_handles = [
            validate_handle(handle) for handle in partitions_handles
        ]

    @property
    def handle(self) -> str:
        return self._handle


class RawVolume(PartitionBasedVolume):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        partitionners = self.partitionners
        if not partitionners.saved or not partitionners.committed_to_os:
            raise ValueError("Given partitioners have not yet set partitions up")

        self.partition_handle = self.partition_handle
        _partitions = partitionners.get_partitions_by_handle(self.partition_handle)
        if len(_partitions) != 1:
            raise ValueError(
                f"Raw Volumes only operates on one partition {len(_partitions)} "
                f"were found for handle {self._handle}"
            )
        self.partition: parted.Partition = _partitions[0]
        self._built = True

    def build(self):
        ...

    @property
    def built(self):
        return self._built

    @property
    def sys_device(self) -> str:
        return self.partition.path


class RaidVolume(PartitionBasedVolume):
    def __init__(
        self,
        *args,
        raid_dev_name: str,
        level: int,
        dev_indices: List[int],
        spare_indices: Union[None, List[int]] = None,
        meta_data: str = "1.2",
        partitionners: HandledPartitions,
        partitions_handle: str,
        handle: str,
    ) -> None:
        super().__init__(
            partitionners=partitionners,
            partitions_handle=partitions_handle,
            handle=handle,
        )
        if not spare_indices:
            self.spare_indices = []
        else:
            self.spare_indices = spare_indices
        self.raid_dev_name = raid_dev_name
        self.dev_indices = dev_indices
        self.set_raid_level(level)
        self.set_meta_data_version(meta_data)
        self._check_devices()
        if not self.raid_dev_name.startswith("/dev/md"):
            raise ValueError(
                "Raid device name should be of the form /dev/md* with * being any number available"
            )
        if os.path.exists(self.raid_dev_name):
            raise LookupError(f"Raid Device {self.raid_dev_name} already exists.")
        if not self.partitionners.saved or not self.partitionners.committed_to_os:
            raise ValueError("Given partitioners have not yet set partitions up")

        if not isinstance(self.partitions_handle, list) and not len(
            self.partitions_handle
        ):
            raise ValueError(
                f"Raw Volumes can only be instanciated with one  partitions_handle"
                f"but {len(self.partitions_handle)} were given."
            )
        self.partition_handle = self.partitions_handle[0]
        self._built = False

    def set_meta_data_version(self, meta_data_version: str):
        meta_data_version = str(meta_data_version)
        if meta_data_version not in ("0", "0.90", "1.0", "1", "1.2"):
            raise ValueError(
                f"You requested meta_data version {self.meta_data_version}, unkown to me."
            )
        self.meta_data_version = meta_data_version

    def set_raid_level(self, raid_level: int):
        if raid_level not in (0, 1, 4, 5, 6, 10):
            raise ValueError(
                f"You requested raid {self.raid_level} level I don't Know this level"
            )
        self.raid_level = raid_level

    def _check_devices(self):
        if self.raid_level == 1 and len(self.dev_indices) != 2:
            raise ValueError(
                f"wrong number of devices for raid {self.raid_level} "
                f"expected 2 got {len(self.dev_indices)}"
            )
        elif self.raid_level == 10 and len(self.dev_indices) != 4:
            raise ValueError(
                f"wrong number of devices for raid {self.raid_level} "
                f"expected 4 got {len(self.dev_indices)}"
            )
        elif self.raid_level in (4, 5, 6) and len(self.dev_indices) < 3:
            raise ValueError(
                f"wrong number of devices for raid {self.raid_level} "
                f"expected >= 3 got {len(self.dev_indices)}"
            )
        devs_set = set(self.dev_indices)
        spares_set = set(self.spare_indices)
        if devs_set.intersection(spares_set):
            raise ValueError(
                "Some devices are common between raid and spare devices this must not happen"
            )
        total_expected_devices = len(self.spare_indices) + len(self.dev_indices)
        total_available_devices = len(
            self.partitionners.get_partitions_by_handle(self.partitions_handle)
        )
        if total_expected_devices != total_available_devices:
            raise ValueError(
                "You expect {total_expected_devices} devices including spares to be cre"
            )

        self.set_devices()

    def set_devices(self):
        part_devices = self.partitionners.get_partitions_by_handle(
            self.partitions_handle
        )
        self.devices = [part_devices[idx] for idx in self.dev_indices]
        self.spares = [part_devices[idx] for idx in self.spare_indices]

    def _gen_build_command(self) -> str:
        command = "mdadm --create {raid_dev_name} --force --level={level} --raid-devices={nb_devs}"
        spares_cmd_switch = "--spare-devices={nb_spares}"
        devs = "{devs}"
        format_params = {
            "raid_dev_name": self.raid_dev_name,
            "level": self.raid_level,
            "nb_devs": len(self.devices),
            "devs": " ".join([dev.path for dev in self.devices]),
        }
        final_command = command
        if self.spares:
            final_command = f"{final_command} {spares_cmd_switch}"
            format_params["nb_spares"] = len(self.spares)
            format_params["devs"] += " "
            format_params["devs"] += " ".join([spare.path for spare in self.spares])
        final_command = f"{final_command} {devs}"
        final_command = final_command.format(**format_params)
        return final_command

    def build(self):
        if self.meta_data_version in ("1.0", "1", "1.2"):
            send_chars = "y\n"
        else:
            send_chars = ""
        command = self._gen_build_command()
        subprocess_command = gen_cmd_for_subprocess(command)
        result: subprocess.CompletedProcess = subprocess.run(
            subprocess_command, capture_output=True, input=send_chars.encode("utf-8")
        )
        if result.returncode != 0:
            raise subprocess.SubprocessError(
                f"Command : {command} Failed ({result.returncode}). {str(result)}"
            )
        if not os.path.exists(self.raid_dev_name):
            raise LookupError(
                f"For some reqson device {self.raid_dev_name} was not created"
            )
        self._built = True

    @property
    def built(self):
        return self._built

    @property
    def sys_device(self) -> str:
        return self.raid_dev_name


class LvmVgVolume(MulitMixedVolume):
    def __init__(
        self,
        *args,
        handled_parts: HandledPartitions,
        partitions_handles: List[str],
        handled_vols: HandledVolumes,
        volumes_handles: List[str],
        handle: str,
    ):
        super().__init__(
            handled_parts=handled_parts,
            partitions_handles=partitions_handles,
            handled_vols=handled_vols,
            volumes_handles=volumes_handles,
            handle=handle,
        )
        self._built = False
        self.devices: Union[None, List[str]]
        self.vg_dev: Union[None, str]

    def ensure_pv(self):
        for pv_dev in self.devices:
            if not os.path.exists(pv_dev):
                raise LookupError(f"Linux device {pv_dev}")
            cmd = subprocess.run(gen_cmd_for_subprocess(f"pvs {pv_dev}"))
            if cmd.returncode != 0:
                cmd = subprocess.run(gen_cmd_for_subprocess(f"pvcreate -f {pv_dev}"))
            if cmd.returncode != 0:
                raise RuntimeError(f"Could not ensure lvm pv on {pv_dev}")
            cmd = subprocess.run(
                gen_cmd_for_subprocess(f"pvdisplay -c {pv_dev}"), capture_output=True
            )
            vg = cmd.stdout.decode("utf-8").strip().splitlines()[-1].split(":")[1]
            if vg:
                raise ValueError(f"Lvm pv already exists ans is attached to vg:{vg}")

    def _set_device_list(self):
        devices: List[str] = [
            str(part.path)
            for handle in self.partitions_handles
            for part in self.handled_parts.get_partitions_by_handle(handle)
        ]
        devices.extend(
            [
                vol.sys_device
                for vol in self.handled_vols.get_by_handles(self.volumes_handles)
            ]
        )
        self.devices = devices

    def build(self):
        self._set_device_list()
        self.ensure_pv()
        command = f"vgcreate {self.handle} {' '.join(self.devices)}"
        cmd = subprocess.run(gen_cmd_for_subprocess(command), capture_output=True)
        if cmd.returncode != 0:
            error = cmd.stderr.decode("utf-8").replace("\n", " -> ")
            raise RuntimeError(f"Unable to Create lvm vg {self.handle} error: {error}")
        self.handled_vols._add_volume(self)
        self.vg_dev = f"/dev/{self.handle}"
        self._built = True

    @property
    def built(self):
        return self._built

    @property
    def sys_device(self) -> str:
        if not self.vg_dev:
            raise RuntimeError("Lvm VG not built yet")
        return self.vg_dev


class LvmLvVolume(VolumeBasedVolume):
    def __init__(
        self,
        *args,
        handled_vols: HandledVolumes,
        volume_handle: str,
        handle: str,
        vg_percent: float,
    ) -> None:
        super().__init__(
            *args, handled_vols=handled_vols, volume_handle=volume_handle, handle=handle
        )
        self.vg_percent = float(vg_percent)
        self.lv_dev: Union[None, str] = None
        self._built = False

    def _vg_has_enough_space(self, vol: Volume) -> Tuple[bool, float]:
        available_percent_command = f"vgdisplay -c {vol.sys_device}"
        available_percent_cmd = subprocess.run(
            gen_cmd_for_subprocess(available_percent_command), capture_output=True
        )
        if available_percent_cmd.returncode != 0:
            raise RuntimeError("Lvm: failed to read vg data")
        output = available_percent_cmd.stdout.decode("utf-8").strip()
        free = int(output.split(":")[-2])
        total = int(output.split(":")[-4])
        available_percent = free / total * 100
        if available_percent < self.vg_percent:
            return False, available_percent
        return True, available_percent

    def build(self):
        vol: Volume = self.get_volume_by_handle()
        if not isinstance(vol, LvmVgVolume):
            raise TypeError(
                "Lvm logical volume support volume must be of type vg -> LvmVgVolume"
            )
        enough_space, available_percent = self._vg_has_enough_space(vol)
        if not enough_space:
            raise SystemError(
                f"Lvm: not enough space available on vg {available_percent:.2f}% "
                f"available, {self.vg_percent:.0f}% requested."
            )
        command = (
            f"lvcreate -l {round(self.vg_percent)}%VG -n {self.handle} {vol.sys_device}"
        )
        cmd = subprocess.run(gen_cmd_for_subprocess(command), capture_output=True)
        if cmd.returncode != 0:
            error = cmd.stderr.decode("utf-8").replace("\n", " -> ")
            raise RuntimeError(
                f"Lvm: Logical Volume creation of {self.handle} on "
                f"vg {vol.sys_device} Failed. Error: {error}"
            )
        self._built = True
        self.lv_dev = f"{vol.sys_device}/{self.handle}"
        self.handled_vols._add_volume(self)

    @property
    def built(self):
        return self._built

    @property
    def sys_device(self) -> str:
        if not self.lv_dev:
            raise RuntimeError("Lvm VG not built yet")
        return self.lv_dev
