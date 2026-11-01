from abc import ABC, abstractmethod
import os
import shlex
import subprocess
from typing import Union

from py_part_recipe.common import gen_cmd_for_subprocess


class FormatError(Exception):
    ...


supported = {}


class FsOps(ABC):
    @abstractmethod
    def make_fs(self):
        ...

    # @abstractmethod
    # def shrink(self, limit: Union[str, None] = None):
    #     ...

    # @abstractmethod
    # def grow(self, limit: Union[str, None] = None):
    #     ...

    # @abstractmethod
    # def wipe(self):
    #     ...


class ExtOps(FsOps):
    def __init__(
        self,
        device: str,
        label: Union[str, None],
        block_size: int = 4096,
        ext_type: str = "ext4",
    ) -> None:
        if ext_type not in ("ext2", "ext3", "ext4"):
            raise ValueError("supported commands are ext2, ext3 or ext4")
        self.command = f"mkfs.{ext_type}"
        self.ext_type = ext_type
        if block_size not in (1024, 2048, 4096):
            raise ValueError(
                f"{self.ext_type.capitalize()} only supports block size in 1024, 2048 or 4096 octets"
            )
        if not label:
            label = device.split("/")[-1][:16]
        label = str(label)[:16]
        if not os.path.exists(device):
            raise ValueError("None existent device device")
        self.device = device
        self.label = label
        self.block_size = block_size

    def make_fs(self):
        command = [
            self.command,
            "-b",
            str(self.block_size),
            "-L",
            self.label,
            self.device,
        ]
        subprocess_command = gen_cmd_for_subprocess(command)
        result: subprocess.CompletedProcess = subprocess.run(
            subprocess_command,
            capture_output=True,
        )
        if result.returncode != 0:
            raise FormatError(
                f"Command : {shlex.join(command)} Failed ({result.returncode}). "
                f"{str(result)}"
            )


class Ext4Ops(ExtOps):
    def __init__(self, device: str, label: str | None, block_size: int = 4096) -> None:
        super().__init__(device, label, block_size, ext_type="ext4")


class Ext3Ops(ExtOps):
    def __init__(self, device: str, label: str | None, block_size: int = 4096) -> None:
        super().__init__(device, label, block_size, ext_type="ext3")


class Ext2Ops(ExtOps):
    def __init__(self, device: str, label: str | None, block_size: int = 4096) -> None:
        super().__init__(device, label, block_size, ext_type="ext2")


supported["ext4"] = Ext4Ops
supported["ext3"] = Ext3Ops
supported["ext2"] = Ext2Ops
supported["ext"] = ExtOps


class FatOps(FsOps):
    def __init__(
        self, device: str, label: Union[str, None], fat_type: int = 32
    ) -> None:
        if fat_type not in (16, 32):
            raise ValueError("FatOps only supports Fat type 16 or 32")
        if not label:
            label = device.split("/")[-1][:11]
        label = str(label)[:11]
        label = label.translate({ord(c): "_" for c in r" !@#$%^&*()[]{};:,./<>?\|`~-=+"})
        if not os.path.exists(device):
            raise ValueError("None existent device device")
        self.device = device
        self.label = label
        self.fat_type = fat_type

    def make_fs(self):
        command = [
            "mkfs.fat",
            "-F",
            str(self.fat_type),
            "-n",
            self.label,
            self.device,
        ]
        subprocess_command = gen_cmd_for_subprocess(command)
        result: subprocess.CompletedProcess = subprocess.run(
            subprocess_command,
            capture_output=True,
        )
        if result.returncode != 0:
            raise FormatError(
                f"Command : {shlex.join(command)} Failed ({result.returncode}). "
                f"{str(result)}"
            )


class Fat16Ops(FatOps):
    def __init__(self, device: str, label: str | None) -> None:
        super().__init__(device, label, fat_type=16)


class Fat32Ops(FatOps):
    def __init__(self, device: str, label: str | None) -> None:
        super().__init__(device, label, fat_type=32)


supported["fat16"] = Fat16Ops
supported["fat32"] = Fat32Ops
supported["fat"] = FatOps
