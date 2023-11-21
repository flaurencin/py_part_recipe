from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import os
import shlex
import subprocess
from typing import Dict, List, Union

import parted
from py_part_recipe.partition_common import HandledPartitions

UID = os.getuid()


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
    def volume_dev(self) -> str:
        ...


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
        self.handle = handle


class VolumeBasedVolume(Volume, ABC):
    def __init__(
        self,
        *args,
        volumes: List[Volume],
        handle: str,
    ) -> None:
        self.volumes = volumes
        self.handle = handle


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
                f"were found for handle {self.handle}"
            )
        self.partition: parted.Partition = _partitions[0]
        self._built = True

    def build(self):
        ...

    @property
    def built(self):
        return self._built

    @property
    def volume_dev(self) -> str:
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
        elif self.raid_level in (4, 5, 6):
            raise ValueError(
                f"wrong number of devices for raid {self.raid_level} "
                f"expected > 3 got {len(self.dev_indices)}"
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

    def _gen_build_command(self, uid: int = UID) -> str:
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
        if uid != 0:
            final_command = f"sudo {final_command}"
        return final_command

    def build(self):
        if self.meta_data_version in ("1.0", "1", "1.2"):
            send_chars = "y\n"
        else:
            send_chars = ""
        command = self._gen_build_command()
        subprocess_command = shlex.split(command)
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
    def volume_dev(self) -> str:
        return self.raid_dev_name


@dataclass
class HandledVolumes:
    volumes: Dict[str, Volume] = field(default_factory=dict)

    @property
    def handle_to_dev(self) -> Dict[str, str]:
        return {handle: vol.volume_dev for handle, vol in self.volumes.items()}

    def build(self):
        for volume in self.volumes.values():
            volume.build()

    def built(self):
        return all([volume.built for volume in self.volumes.values()])
