"""Apply a recipe: create what it describes and report what exists.

The stages run in this order, each one needs the previous:

1. `partitioning`: the disks are wiped if asked, partitioned, and the kernel
   is told about the new partitions
2. `volumes`: raw, RAID and LVM volumes are created, in the order of the recipe
3. `file_systems`: the volumes that must be formatted are formatted
4. `report`: the system is read to describe what was created

Applying a recipe is destructive and **nothing is undone** when a stage fails:
the run stops with an ApplyError saying which stage failed, why, and what was
already changed. Cleaning up, or deciding what to do with the partial result,
is up to the caller.

Everything that can be checked before the first write is checked first: the
disks exist and are block devices, the partitions fit in them, and the
volumes can be set up on the partitions.
"""

from typing import Dict, List, Optional

from py_part_recipe.fs import supported
from py_part_recipe.partition_common import HandledPartitions, Partitionner, Recipe
from py_part_recipe.recipe_loader import (
    LogicalVolumeParams,
    RaidParams,
    RawParams,
    RecipeSpec,
    VolumeGroupParams,
    VolumeSpec,
)
from py_part_recipe.report import Report
from py_part_recipe.report_builder import FileSystemEntry, build_report
from py_part_recipe.volume_common import (
    HandledVolumes,
    LvmLvVolume,
    LvmVgVolume,
    RaidVolume,
    RawVolume,
    Volume,
)
from py_part_recipe.wipe import settle

STAGES = ("partitioning", "volumes", "file_systems", "report")


class ApplyError(Exception):
    """A stage failed, and nothing was undone.

    Attributes:
        stage (str): the stage that failed, one of STAGES
        cause (Exception): what went wrong
        created (dict): what was changed before the failure, by kind:
            "disks modified", "volumes built", "file systems formatted"
    """

    def __init__(
        self, stage: str, cause: Exception, created: Dict[str, List[str]]
    ) -> None:
        self.stage = stage
        self.cause = cause
        self.created = {kind: list(items) for kind, items in created.items() if items}
        message = (
            f"Applying the recipe failed at stage '{stage}': "
            f"{type(cause).__name__}: {cause}. Nothing was undone."
        )
        for kind, items in self.created.items():
            message += f" {kind.capitalize()}: {', '.join(items)}."
        super().__init__(message)


class Applier:
    """Apply a recipe stage by stage.

    `run()` does it all. The stages can also be called one by one, in order,
    by a caller who wants to act between them.
    """

    def __init__(self, recipe: RecipeSpec) -> None:
        self.recipe = recipe
        self.partitions: Optional[HandledPartitions] = None
        self.volumes = HandledVolumes()
        self.disks_modified: List[str] = []
        self.formatted: List[str] = []
        self.file_systems: List[FileSystemEntry] = []
        self.report: Optional[Report] = None

    # ----- what exists, for the error

    def created(self) -> Dict[str, List[str]]:
        return {
            "disks modified": list(self.disks_modified),
            "volumes built": [
                handle
                for handle, volume in self.volumes.volumes.items()
                if volume.built and not isinstance(volume, RawVolume)
            ],
            "file systems formatted": list(self.formatted),
        }

    def _stage(self, name: str, action):
        try:
            return action()
        except Exception as error:
            raise ApplyError(name, error, self.created()) from error

    # ----- stages

    def partition(self):
        """Wipe (if asked), partition the disks, tell the kernel."""
        self._stage("partitioning", self._partition)

    def _partition(self):
        # nothing is written until every group is known to be possible:
        # the disks are checked and the partitions mapped in memory first
        partitionners = [
            Partitionner(
                Recipe(
                    group.devices,
                    group.partitions,
                    keep_partitions=group.keep_partitions,
                    wipe=group.wipe,
                )
            )
            for group in self.recipe.partitioning
        ]
        partitions = HandledPartitions(partitionners)
        partitions.create_partitions_mapping()
        self.partitions = partitions
        # from here on, the disks may have been changed
        self.disks_modified = [
            device.path
            for partitionner in partitionners
            for device in partitionner.recipe.devices
        ]
        partitions.commit_to_devices()
        partitions.notify_kernel()
        partitions.wipe_partitions()

    def build_volumes(self):
        """Create the volumes, in the order of the recipe."""
        self._stage("volumes", self._build_volumes)

    def _volumes_on_wiped_disks(self) -> set:
        """Handles of the volumes built, even indirectly, on disks that are wiped.

        Wiping the partitions is not enough for them: the data of an array
        starts at the same place of its partitions as the one it replaces
        (and a logical volume gets the extents of the previous one), so what
        was there, like an lvm physical volume, shows up again. They are wiped
        too when they are created.
        """
        wiped_partitions = {
            part.handle
            for group in self.recipe.partitioning
            if group.wipe
            for part in group.partitions
        }
        wiped = set()
        for spec in self.recipe.volumes:
            params = spec.params
            if isinstance(params, (RawParams, RaidParams)):
                on_wiped = params.partitions_handle in wiped_partitions
            elif isinstance(params, VolumeGroupParams):
                on_wiped = any(
                    handle in wiped_partitions for handle in params.partitions_handles
                ) or any(handle in wiped for handle in params.volumes_handles)
            else:
                on_wiped = params.volume_handle in wiped
            if on_wiped:
                wiped.add(spec.handle)
        return wiped

    def _volume(self, spec: VolumeSpec, wiped: set) -> Volume:
        params = spec.params
        if isinstance(params, RawParams):
            return RawVolume(
                partitionners=self.partitions,
                partitions_handle=params.partitions_handle,
                handle=spec.handle,
            )
        if isinstance(params, RaidParams):
            return RaidVolume(
                partitionners=self.partitions,
                partitions_handle=params.partitions_handle,
                handle=spec.handle,
                raid_dev_name=params.raid_dev_name,
                level=params.level,
                dev_indices=params.dev_indices,
                spare_indices=params.spare_indices,
                meta_data=params.meta_data,
                wipe=spec.handle in wiped,
            )
        if isinstance(params, VolumeGroupParams):
            return LvmVgVolume(
                handled_parts=self.partitions,
                partitions_handles=params.partitions_handles,
                handled_vols=self.volumes,
                volumes_handles=params.volumes_handles,
                handle=spec.handle,
            )
        if isinstance(params, LogicalVolumeParams):
            return LvmLvVolume(
                handled_vols=self.volumes,
                volume_handle=params.volume_handle,
                vg_percent=params.vg_percent,
                handle=spec.handle,
                wipe=spec.handle in wiped,
            )
        raise TypeError(f"Unknown parameters of volume {spec.handle}: {params!r}")

    def _build_volumes(self):
        if self.partitions is None:
            raise RuntimeError("The disks must be partitioned before the volumes")
        # all the volumes are checked against the partitions before any is built
        wiped = self._volumes_on_wiped_disks()
        for spec in self.recipe.volumes:
            self.volumes.add(self._volume(spec, wiped))
        self.volumes.build()
        settle()

    def format_file_systems(self):
        """Format the volumes the recipe asks to format."""
        self._stage("file_systems", self._format_file_systems)

    def _format_file_systems(self):
        for spec in self.recipe.file_systems:
            volume = self.volumes.get_by_handle(spec.volume_handle)
            if spec.format:
                supported[spec.type](volume.sys_device, spec.label).make_fs()
                self.formatted.append(spec.volume_handle)
            self.file_systems.append(
                FileSystemEntry(
                    volume=spec.volume_handle,
                    type=spec.type,
                    formatted=spec.format,
                    mount=spec.mount,
                    mount_options=spec.mount_options,
                    fsck_pass=spec.fsck_pass,
                )
            )
        settle()

    def build_report(self) -> Report:
        """Read the system, describe what exists."""
        self.report = self._stage(
            "report",
            lambda: build_report(self.partitions, self.volumes, self.file_systems),
        )
        return self.report

    def run(self) -> Report:
        """Run every stage, return the report.

        Raises:
            ApplyError: a stage failed; nothing is undone
        """
        self.partition()
        self.build_volumes()
        self.format_file_systems()
        return self.build_report()


def apply_recipe(recipe: RecipeSpec) -> Report:
    """Apply a recipe read with load_recipe(), return the report."""
    return Applier(recipe).run()
