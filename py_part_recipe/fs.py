from abc import ABC, abstractmethod
import os
import shlex
import subprocess
from typing import Dict, List, Type, Union

from py_part_recipe.common import gen_cmd_for_subprocess


class FormatError(Exception): ...


supported: Dict[str, Type["FsOps"]] = {}


def truncate_label(label: str, limit: int, in_bytes: bool = True) -> str:
    """Cut a label to the limit of its file system.

    Some file systems count characters, others count bytes (utf-8), in which
    case a multi bytes character is dropped entirely rather than cut in half.
    """
    if not in_bytes:
        return label[:limit]
    return label.encode("utf-8")[:limit].decode("utf-8", errors="ignore")


class FsOps(ABC):
    """Format a device with a file system.

    Formatting is destructive: file systems that refuse to overwrite an
    existing one are forced to, the caller decides if a device must be
    formatted.

    Subclasses give the label limit of the file system and the command line
    that formats the device.
    """

    label_limit: int
    label_in_bytes: bool = True

    def __init__(self, device: str, label: Union[str, None] = None) -> None:
        if not os.path.exists(device):
            raise ValueError(f"Device {device} does not exist")
        self.device = device
        if not label:
            label = device.split("/")[-1]
        self.label = self._make_label(str(label))

    def _make_label(self, label: str) -> str:
        return truncate_label(label, self.label_limit, self.label_in_bytes)

    @abstractmethod
    def _command(self) -> List[str]:
        """Command line, as a list so a label can contain spaces."""

    def make_fs(self):
        command = self._command()
        result: subprocess.CompletedProcess = subprocess.run(
            gen_cmd_for_subprocess(command),
            capture_output=True,
        )
        if result.returncode != 0:
            raise FormatError(
                f"Command : {shlex.join(command)} Failed ({result.returncode}). "
                f"{str(result)}"
            )


class ExtOps(FsOps):
    label_limit = 16

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
        self.block_size = block_size
        super().__init__(device, label)

    def _command(self) -> List[str]:
        return [
            self.command,
            "-b",
            str(self.block_size),
            "-L",
            self.label,
            self.device,
        ]


class Ext4Ops(ExtOps):
    def __init__(
        self, device: str, label: Union[str, None], block_size: int = 4096
    ) -> None:
        super().__init__(device, label, block_size, ext_type="ext4")


class Ext3Ops(ExtOps):
    def __init__(
        self, device: str, label: Union[str, None], block_size: int = 4096
    ) -> None:
        super().__init__(device, label, block_size, ext_type="ext3")


class Ext2Ops(ExtOps):
    def __init__(
        self, device: str, label: Union[str, None], block_size: int = 4096
    ) -> None:
        super().__init__(device, label, block_size, ext_type="ext2")


supported["ext4"] = Ext4Ops
supported["ext3"] = Ext3Ops
supported["ext2"] = Ext2Ops
supported["ext"] = ExtOps


class FatOps(FsOps):
    label_limit = 11

    def __init__(
        self, device: str, label: Union[str, None], fat_type: int = 32
    ) -> None:
        if fat_type not in (16, 32):
            raise ValueError("FatOps only supports Fat type 16 or 32")
        self.fat_type = fat_type
        super().__init__(device, label)

    def _make_label(self, label: str) -> str:
        label = super()._make_label(label)
        return label.translate({ord(c): "_" for c in r" !@#$%^&*()[]{};:,./<>?\|`~-=+"})

    def _command(self) -> List[str]:
        return [
            "mkfs.fat",
            "-F",
            str(self.fat_type),
            "-n",
            self.label,
            self.device,
        ]


class Fat16Ops(FatOps):
    def __init__(self, device: str, label: Union[str, None]) -> None:
        super().__init__(device, label, fat_type=16)


class Fat32Ops(FatOps):
    def __init__(self, device: str, label: Union[str, None]) -> None:
        super().__init__(device, label, fat_type=32)


supported["fat16"] = Fat16Ops
supported["fat32"] = Fat32Ops
supported["fat"] = FatOps


class XfsOps(FsOps):
    label_limit = 12

    def _command(self) -> List[str]:
        return ["mkfs.xfs", "-f", "-L", self.label, self.device]


class BtrfsOps(FsOps):
    # mkfs.btrfs accepts 255 bytes but blkid and btrfs read back at most 254
    label_limit = 254

    def _command(self) -> List[str]:
        return ["mkfs.btrfs", "-f", "-L", self.label, self.device]


class ExfatOps(FsOps):
    label_limit = 11
    label_in_bytes = False

    def _command(self) -> List[str]:
        return ["mkfs.exfat", "-L", self.label, self.device]


class NtfsOps(FsOps):
    label_limit = 128
    label_in_bytes = False

    def _command(self) -> List[str]:
        # quick format; force, as the device may be a whole disk
        return ["mkfs.ntfs", "-Q", "-F", "-L", self.label, self.device]


class SwapOps(FsOps):
    """Prepare swap space. It is never activated (no swapon)."""

    label_limit = 15

    def _command(self) -> List[str]:
        return ["mkswap", "-L", self.label, self.device]


supported["xfs"] = XfsOps
supported["btrfs"] = BtrfsOps
supported["exfat"] = ExfatOps
supported["ntfs"] = NtfsOps
supported["swap"] = SwapOps
