"""Lock snapshot service.

Provides batch snapshots for :mod:`meltano.cli.lock`: every plugin target of a
batch is resolved individually into a staging directory, and a self-contained,
versioned snapshot is published atomically once all targets have resolved.
Readers therefore observe either the previous snapshot or the complete new
snapshot, never a mixture of the two.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import typing as t
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone

import fasteners
from structlog.stdlib import get_logger

from meltano.core.error import MeltanoError
from meltano.core.plugin.base import PluginType, StandalonePlugin
from meltano.core.project_dirs_service import LOCK_SNAPSHOTS_DIRNAME

if t.TYPE_CHECKING:
    from pathlib import Path

    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

logger = get_logger(__name__)

#: Version of the on-disk snapshot/staging format used by this module.
SNAPSHOT_FORMAT_VERSION = 1

# Definition provenance kinds.
PROVENANCE_HUB = "hub"
PROVENANCE_LOCAL = "local"

# Snapshot entry kinds.
ENTRY_LOCKED = "locked"
ENTRY_INHERITED = "inherited"
ENTRY_CUSTOM = "custom"
ENTRY_LEGACY = "legacy"

# Snapshot lifecycle states, recorded in the staging index.
STATE_RESOLVING = "resolving"
STATE_STAGED = "staged"
STATE_PUBLISHING = "publishing"

# Per-item resolution statuses, recorded in the staging index.
ITEM_PENDING = "pending"
ITEM_SUCCESS = "success"
ITEM_FAILED = "failed"

# Item results as reported to the user.
RESULT_LOCKED = "locked"
RESULT_REUSED = "reused"
RESULT_FAILED = "failed"
RESULT_SKIPPED = "skipped"


class SnapshotError(MeltanoError):
    """Base class for lock snapshot errors."""


class SnapshotExistsError(SnapshotError):
    """Raised when a snapshot is already published and no update was requested."""

    def __init__(self, snapshot_id: str):
        """Create a new SnapshotExistsError.

        Args:
            snapshot_id: The ID of the already published snapshot.
        """
        super().__init__(
            f"Lock snapshot {snapshot_id!r} is already published",
            "Re-run with '--update' to publish a new snapshot",
        )


class SnapshotConflictError(SnapshotError):
    """Raised when a concurrent batch moved the pointer or project version."""

    def __init__(
        self,
        *,
        expected_snapshot_id: str | None,
        actual_snapshot_id: str | None,
        project_version_changed: bool,
    ):
        """Create a new SnapshotConflictError.

        Args:
            expected_snapshot_id: The pointer ID this batch was based on.
            actual_snapshot_id: The pointer ID found when publishing.
            project_version_changed: Whether the project version changed.
        """
        if project_version_changed:
            reason = "The project changed while this batch was being prepared"
        else:
            reason = (
                f"Another batch published lock snapshot {actual_snapshot_id!r} "
                f"while this batch was being prepared (expected "
                f"{expected_snapshot_id!r})"
            )

        super().__init__(
            reason,
            "Re-run the command to resolve the plugins against the latest "
            "project state",
        )
        self.expected_snapshot_id = expected_snapshot_id
        self.actual_snapshot_id = actual_snapshot_id
        self.project_version_changed = project_version_changed


@dataclass(frozen=True)
class DefinitionProvenance:
    """Provenance of a frozen plugin definition."""

    kind: str
    """Whether the definition was resolved from the Hub or from a local source."""

    origin: str
    """Identity of the source: the Hub API root URL, or a local origin label."""

    ref: str | None = None
    """For Hub-sourced definitions, the URL of the definition resource."""

    def canonical(self) -> dict[str, t.Any]:
        """Serialize to a plain dictionary.

        Returns:
            The provenance as a dictionary.
        """
        result = {"kind": self.kind, "origin": self.origin}
        if self.ref is not None:
            result["ref"] = self.ref
        return result

    @classmethod
    def parse(cls, data: dict[str, t.Any]) -> DefinitionProvenance:
        """Parse from a plain dictionary.

        Args:
            data: The dictionary to parse.

        Returns:
            The parsed provenance.
        """
        return cls(kind=data["kind"], origin=data["origin"], ref=data.get("ref"))


@dataclass(frozen=True)
class SnapshotEntry:
    """One entry in a lock snapshot."""

    type: str
    """The plugin type value, e.g. `extractors`."""

    name: str
    """The root plugin name (the name a non-inherited definition is known by)."""

    kind: str
    """One of `ENTRY_LOCKED`, `ENTRY_INHERITED`, `ENTRY_CUSTOM`, `ENTRY_LEGACY`."""

    variant: str | None = None
    """The resolved variant name; `None` only for variantless legacy locks."""

    file: str | None = None
    """Path of the lock file, relative to the project root."""

    inherit_from: str | None = None
    """For inherited entries, the name of the plugin they inherit from."""

    provenance: DefinitionProvenance | None = None
    """Where the frozen definition came from."""

    is_default_variant: bool | None = None
    """Whether the entry is for the default variant of the plugin."""

    is_deprecated: bool | None = None
    """Whether the variant is deprecated."""

    def matches(
        self,
        *,
        plugin_type: str,
        plugin_name: str,
        variant_name: str | None,
    ) -> bool:
        """Whether this entry identifies the requested plugin and variant.

        Args:
            plugin_type: The requested plugin type value.
            plugin_name: The requested plugin name.
            variant_name: The requested variant name; `None` matches the
                default-variant entry.

        Returns:
            Whether the entry matches the request.
        """
        if self.type != plugin_type or self.name != plugin_name:
            return False

        if variant_name is None:
            return self.is_default_variant is True or self.variant is None

        return self.variant == variant_name

    def canonical(self) -> dict[str, t.Any]:
        """Serialize to a plain dictionary.

        Returns:
            The entry as a dictionary.
        """
        result: dict[str, t.Any] = {
            "type": self.type,
            "name": self.name,
            "kind": self.kind,
        }
        if self.variant is not None:
            result["variant"] = self.variant
        if self.file is not None:
            result["file"] = self.file
        if self.inherit_from is not None:
            result["inherit_from"] = self.inherit_from
        if self.provenance is not None:
            result["provenance"] = self.provenance.canonical()
        if self.is_default_variant is not None:
            result["is_default_variant"] = self.is_default_variant
        if self.is_deprecated is not None:
            result["is_deprecated"] = self.is_deprecated
        return result

    @classmethod
    def parse(cls, data: dict[str, t.Any]) -> SnapshotEntry:
        """Parse from a plain dictionary.

        Args:
            data: The dictionary to parse.

        Returns:
            The parsed entry.
        """
        provenance = data.get("provenance")
        return cls(
            type=data["type"],
            name=data["name"],
            kind=data["kind"],
            variant=data.get("variant"),
            file=data.get("file"),
            inherit_from=data.get("inherit_from"),
            provenance=(
                DefinitionProvenance.parse(provenance)
                if provenance is not None
                else None
            ),
            is_default_variant=data.get("is_default_variant"),
            is_deprecated=data.get("is_deprecated"),
        )


@dataclass(frozen=True)
class LockSnapshotManifest:
    """The content of a published lock snapshot pointer."""

    snapshot_id: str
    """The unique ID of the snapshot."""

    project_version: str
    """SHA-256 of the `meltano.yml` the snapshot was resolved from."""

    environment: str | None
    """The name of the active environment, if any."""

    created_at: str
    """ISO-8601 timestamp of publication."""

    entries: list[SnapshotEntry]
    """Every entry frozen in the snapshot."""

    format_version: int = SNAPSHOT_FORMAT_VERSION
    """The on-disk format version."""

    def find_entry(
        self,
        *,
        plugin_type: str,
        plugin_name: str,
        variant_name: str | None,
    ) -> SnapshotEntry | None:
        """Find the entry for a plugin and variant, if one is listed.

        Args:
            plugin_type: The requested plugin type value.
            plugin_name: The requested plugin name.
            variant_name: The requested variant name.

        Returns:
            The matching entry, or `None`.
        """
        for entry in self.entries:
            if entry.matches(
                plugin_type=plugin_type,
                plugin_name=plugin_name,
                variant_name=variant_name,
            ):
                return entry
        return None

    def canonical(self) -> dict[str, t.Any]:
        """Serialize to a plain dictionary.

        Returns:
            The manifest as a dictionary.
        """
        return {
            "format_version": self.format_version,
            "snapshot_id": self.snapshot_id,
            "project_version": self.project_version,
            "environment": self.environment,
            "created_at": self.created_at,
            "entries": [entry.canonical() for entry in self.entries],
        }

    @classmethod
    def parse(cls, data: dict[str, t.Any]) -> LockSnapshotManifest:
        """Parse from a plain dictionary.

        Args:
            data: The dictionary to parse.

        Returns:
            The parsed manifest.
        """
        return cls(
            snapshot_id=data["snapshot_id"],
            project_version=data["project_version"],
            environment=data.get("environment"),
            created_at=data["created_at"],
            entries=[SnapshotEntry.parse(entry) for entry in data["entries"]],
            format_version=data.get("format_version", SNAPSHOT_FORMAT_VERSION),
        )


@dataclass
class StagingItem:
    """One item in a batch staging index."""

    key: str
    """Stable key of the target, `<type>/<root-name>[--<variant>]`."""

    type: str
    """The plugin type value."""

    name: str
    """The root plugin name."""

    variant: str | None
    """The variant as declared by the plugin, `None` for the default variant."""

    fingerprint: str
    """Fingerprint of the resolution inputs."""

    status: str
    """One of `ITEM_PENDING`, `ITEM_SUCCESS`, `ITEM_FAILED`."""

    kind: str = ENTRY_LOCKED
    """One of `ENTRY_LOCKED`, `ENTRY_INHERITED`, `ENTRY_CUSTOM`."""

    inherit_from: str | None = None
    """For inherited items, the name of the plugin they inherit from."""

    output: str | None = None
    """Path of the resolved lock file, relative to the staging directory."""

    resolved_variant: str | None = None
    """The variant name resolved from the Hub, when different from `variant`."""

    provenance: DefinitionProvenance | None = None
    """Where the resolved definition came from."""

    is_default_variant: bool | None = None
    """Whether the resolved variant is the default one."""

    is_deprecated: bool | None = None
    """Whether the resolved variant is deprecated."""

    error: str | None = None
    """For failed items, the error message."""

    def canonical(self) -> dict[str, t.Any]:
        """Serialize to a plain dictionary.

        Returns:
            The item as a dictionary.
        """
        result: dict[str, t.Any] = {
            "key": self.key,
            "type": self.type,
            "name": self.name,
            "variant": self.variant,
            "fingerprint": self.fingerprint,
            "status": self.status,
            "kind": self.kind,
        }
        if self.inherit_from is not None:
            result["inherit_from"] = self.inherit_from
        if self.output is not None:
            result["output"] = self.output
        if self.resolved_variant is not None:
            result["resolved_variant"] = self.resolved_variant
        if self.provenance is not None:
            result["provenance"] = self.provenance.canonical()
        if self.is_default_variant is not None:
            result["is_default_variant"] = self.is_default_variant
        if self.is_deprecated is not None:
            result["is_deprecated"] = self.is_deprecated
        if self.error is not None:
            result["error"] = self.error
        return result

    @classmethod
    def parse(cls, data: dict[str, t.Any]) -> StagingItem:
        """Parse from a plain dictionary.

        Args:
            data: The dictionary to parse.

        Returns:
            The parsed item.
        """
        provenance = data.get("provenance")
        return cls(
            key=data["key"],
            type=data["type"],
            name=data["name"],
            variant=data.get("variant"),
            fingerprint=data["fingerprint"],
            status=data["status"],
            kind=data.get("kind", ENTRY_LOCKED),
            inherit_from=data.get("inherit_from"),
            output=data.get("output"),
            resolved_variant=data.get("resolved_variant"),
            provenance=(
                DefinitionProvenance.parse(provenance)
                if provenance is not None
                else None
            ),
            is_default_variant=data.get("is_default_variant"),
            is_deprecated=data.get("is_deprecated"),
            error=data.get("error"),
        )


@dataclass
class Staging:
    """A batch staging index."""

    snapshot_id: str
    """The ID of the snapshot this staging will publish."""

    state: str
    """One of `STATE_RESOLVING`, `STATE_STAGED`, `STATE_PUBLISHING`."""

    pid: int
    """The PID of the process that owns this staging."""

    created_at: str
    """ISO-8601 timestamp of creation."""

    base_snapshot_id: str | None
    """The pointer ID this batch was based on, if any."""

    base_project_version: str
    """The project version this batch was based on."""

    hub_origin: str
    """The Hub origin this batch was based on."""

    items: dict[str, StagingItem] = field(default_factory=dict)
    """The items of the batch, keyed by `StagingItem.key`."""

    format_version: int = SNAPSHOT_FORMAT_VERSION
    """The on-disk format version."""

    def canonical(self) -> dict[str, t.Any]:
        """Serialize to a plain dictionary.

        Returns:
            The staging index as a dictionary.
        """
        return {
            "format_version": self.format_version,
            "snapshot_id": self.snapshot_id,
            "state": self.state,
            "pid": self.pid,
            "created_at": self.created_at,
            "base_snapshot_id": self.base_snapshot_id,
            "base_project_version": self.base_project_version,
            "hub_origin": self.hub_origin,
            "items": [item.canonical() for item in self.items.values()],
        }

    @classmethod
    def parse(cls, data: dict[str, t.Any]) -> Staging:
        """Parse from a plain dictionary.

        Args:
            data: The dictionary to parse.

        Returns:
            The parsed staging index.
        """
        items = [StagingItem.parse(item) for item in data.get("items", [])]
        return cls(
            snapshot_id=data["snapshot_id"],
            state=data["state"],
            pid=data["pid"],
            created_at=data["created_at"],
            base_snapshot_id=data.get("base_snapshot_id"),
            base_project_version=data["base_project_version"],
            hub_origin=data["hub_origin"],
            items={item.key: item for item in items},
            format_version=data.get("format_version", SNAPSHOT_FORMAT_VERSION),
        )


@dataclass(frozen=True)
class ItemResult:
    """The resolution result for one batch item."""

    key: str
    """The item key, see `StagingItem.key`."""

    descriptor: str
    """A human-readable descriptor, e.g. `extractor tap-mock`."""

    kind: str
    """The entry kind, one of the `ENTRY_*` constants."""

    status: str
    """One of `RESULT_LOCKED`, `RESULT_REUSED`, `RESULT_FAILED`, `RESULT_SKIPPED`."""

    variant: str | None = None
    """The resolved variant name, if any."""

    error: str | None = None
    """For failed items, the error message."""


@dataclass
class BatchReport:
    """The aggregate result of a lock batch."""

    snapshot_id: str
    """The ID of the snapshot the batch prepared."""

    results: list[ItemResult]
    """One result per batch item."""

    published: bool = False
    """Whether the snapshot was published."""

    @property
    def failed_results(self) -> list[ItemResult]:
        """The failed item results."""
        return [result for result in self.results if result.status == RESULT_FAILED]

    @property
    def has_failures(self) -> bool:
        """Whether any item failed."""
        return any(result.status == RESULT_FAILED for result in self.results)


def compute_fingerprint(
    *,
    plugin_type: str,
    plugin_name: str,
    variant: str | None,
    hub_origin: str,
    local_definition: t.Mapping[str, t.Any] | None = None,
) -> str:
    """Compute the fingerprint of a plugin resolution's inputs.

    The fingerprint is stable for identical inputs and changes whenever the
    plugin type, root name, variant, or Hub origin changes.

    Args:
        plugin_type: The plugin type value.
        plugin_name: The root plugin name.
        variant: The variant as declared, `None` for the default variant.
        hub_origin: The configured Hub API root URL.
        local_definition: For locally defined plugins, their canonical content.

    Returns:
        The SHA-256 fingerprint.
    """
    payload: dict[str, t.Any] = {
        "type": plugin_type,
        "name": plugin_name,
        "variant": variant,
        "hub_origin": hub_origin,
    }
    if local_definition is not None:
        payload["local_definition"] = dict(local_definition)

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _pid_is_alive(pid: int) -> bool:
    """Whether a process with the given PID exists.

    Args:
        pid: The PID to check.

    Returns:
        Whether the process is alive.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The process exists, but we may not signal it.
        return True
    return True


class LockSnapshotService:
    """Service for batch-resolving and atomically publishing lock snapshots."""

    def __init__(self, project: Project):
        """Create a new LockSnapshotService.

        Args:
            project: The Meltano project.
        """
        self.project = project

    @property
    def pointer_path(self) -> Path:
        """The path to the published snapshot pointer."""
        return self.project.dirs.plugin_lock_snapshot_pointer()

    def snapshot_dir(self, snapshot_id: str) -> Path:
        """Get the directory of a snapshot.

        Args:
            snapshot_id: The snapshot ID.

        Returns:
            The snapshot directory path.
        """
        return self.project.dirs.plugin_lock_snapshots_dir(snapshot_id)

    def staging_dir(self, snapshot_id: str) -> Path:
        """Get the staging directory of a snapshot.

        Args:
            snapshot_id: The snapshot ID.

        Returns:
            The staging directory path.
        """
        return self.project.dirs.plugin_lock_staging_dir(snapshot_id)

    @property
    def batch_lock_path(self) -> Path:
        """The path to the batch publish interprocess lock file."""
        return self.project.dirs.plugin_lock_batch_lock_path()

    def project_version(self) -> str:
        """Compute the current project version.

        Returns:
            The SHA-256 of the `meltano.yml` file contents.
        """
        return hashlib.sha256(self.project.meltanofile.read_bytes()).hexdigest()

    @property
    def hub_origin(self) -> str:
        """The configured Hub API root URL."""
        return self.project.hub_service.hub_api_url

    def read_manifest(self) -> LockSnapshotManifest | None:
        """Read the published snapshot manifest, if any.

        Returns:
            The manifest, or `None` when no pointer exists.
        """
        if not self.pointer_path.exists():
            return None
        return LockSnapshotManifest.parse(json.loads(self.pointer_path.read_text()))

    # --- Staging index I/O ---

    def staging_index_path(self, snapshot_id: str) -> Path:
        """Get the path to a staging index file.

        Args:
            snapshot_id: The snapshot ID.

        Returns:
            The staging index path.
        """
        return self.staging_dir(snapshot_id) / "staging.json"

    def _write_staging(self, staging: Staging) -> None:
        """Write a staging index, replacing its file atomically.

        Args:
            staging: The staging index to write.
        """
        path = self.staging_index_path(staging.snapshot_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"staging.json.{os.getpid()}.tmp")
        tmp.write_text(
            json.dumps(staging.canonical(), indent=2) + "\n",
        )
        tmp.replace(path)

    def iter_stagings(self) -> list[Staging]:
        """List all staging indexes, newest first.

        Returns:
            The staging indexes found.
        """
        root = self.project.dirs.plugin_lock_staging_dir()
        if not root.exists():
            return []

        stagings: list[Staging] = []
        for path in root.iterdir():
            index = path / "staging.json"
            if not index.exists():
                continue
            with suppress(Exception):
                stagings.append(Staging.parse(json.loads(index.read_text())))

        return sorted(
            stagings,
            key=lambda staging: staging.created_at,
            reverse=True,
        )

    # --- Classification ---

    @staticmethod
    def classify(plugin: ProjectPlugin) -> str:
        """Classify a plugin into a snapshot entry kind.

        Args:
            plugin: The plugin to classify.

        Returns:
            One of `ENTRY_CUSTOM`, `ENTRY_INHERITED`, `ENTRY_LOCKED`.
        """
        if plugin.is_custom():
            return ENTRY_CUSTOM
        if plugin.inherit_from is not None:
            return ENTRY_INHERITED
        return ENTRY_LOCKED

    @staticmethod
    def item_key(plugin: ProjectPlugin) -> str:
        """Compute the stable key of a plugin item.

        Root plugins are keyed by name and variant; inherited and custom
        plugins are keyed by their own name.

        Args:
            plugin: The plugin to key.

        Returns:
            The key, `<type>/<name>[--<variant>]`.
        """
        if plugin.is_custom() or plugin.inherit_from is not None:
            return f"{plugin.type.value}/{plugin.name}"

        suffix = f"--{plugin.variant}" if plugin.variant else ""
        return f"{plugin.type.value}/{plugin.name}{suffix}"

    @staticmethod
    def _descriptor(item: StagingItem) -> str:
        return f"{PluginType(item.type).descriptor} {item.name}"

    # --- Batch lifecycle ---

    def begin_batch(
        self,
        base: LockSnapshotManifest | None,
    ) -> Staging:
        """Create a new staging index for a batch.

        Args:
            base: The manifest the batch is based on, if any.

        Returns:
            The new staging index.
        """
        staging = Staging(
            snapshot_id=uuid.uuid4().hex,
            state=STATE_RESOLVING,
            pid=os.getpid(),
            created_at=self._now(),
            base_snapshot_id=base.snapshot_id if base is not None else None,
            base_project_version=self.project_version(),
            hub_origin=self.hub_origin,
        )
        self._write_staging(staging)
        logger.info("Preparing lock snapshot", snapshot_id=staging.snapshot_id)
        return staging

    def add_item(
        self,
        staging: Staging,
        plugin: ProjectPlugin,
    ) -> StagingItem:
        """Add a plugin as a staging item.

        Args:
            staging: The staging index.
            plugin: The plugin to add.

        Returns:
            The added staging item.
        """
        kind = self.classify(plugin)
        item = StagingItem(
            key=self.item_key(plugin),
            type=plugin.type.value,
            name=plugin.name,
            variant=plugin.variant,
            fingerprint=compute_fingerprint(
                plugin_type=plugin.type.value,
                plugin_name=plugin.name,
                variant=plugin.variant,
                hub_origin=self.hub_origin,
            ),
            status=ITEM_PENDING,
            kind=kind,
            inherit_from=plugin.inherit_from,
        )
        staging.items[item.key] = item
        return item

    def _try_reuse(self, staging: Staging, item: StagingItem) -> bool:
        """Reuse a successful item from prior staging, when its inputs match.

        Args:
            staging: The current staging index.
            item: The item to resolve.

        Returns:
            Whether a prior resolution was reused.
        """
        for prior in self.iter_stagings():
            if prior.snapshot_id == staging.snapshot_id:
                continue

            prior_item = prior.items.get(item.key)
            if not (
                prior_item is not None
                and prior_item.status == ITEM_SUCCESS
                and prior_item.fingerprint == item.fingerprint
                and prior_item.output
            ):
                continue

            source = self.staging_dir(prior.snapshot_id) / prior_item.output
            if not source.exists():
                continue

            resolved_variant = prior_item.resolved_variant or prior_item.variant
            output = f"plugins/{item.type}/{item.name}--{resolved_variant}.lock"
            target = self.staging_dir(staging.snapshot_id) / output
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())

            item.output = output
            item.resolved_variant = prior_item.resolved_variant
            item.provenance = prior_item.provenance
            item.is_default_variant = prior_item.is_default_variant
            item.is_deprecated = prior_item.is_deprecated
            item.status = ITEM_SUCCESS
            item.error = None

            logger.info(
                "Reusing resolved plugin definition",
                key=item.key,
                from_snapshot=prior.snapshot_id,
            )
            return True
        return False

    def _resolve_from_hub(
        self,
        staging: Staging,
        item: StagingItem,
    ) -> None:
        """Resolve one item from the Hub and write its lock file.

        Args:
            staging: The staging index.
            item: The item to resolve.
        """
        plugin_type = PluginType(item.type)
        definition = self.project.hub_service.find_definition(
            plugin_type,
            item.name,
            variant_name=item.variant,
        )
        variant_obj = definition.find_variant(item.variant)
        content = (
            json.dumps(
                StandalonePlugin.from_variant(
                    variant_obj,
                    definition,
                ).canonical(),
                indent=2,
            )
            + "\n"
        )

        output = f"plugins/{item.type}/{item.name}--{variant_obj.name}.lock"
        target = self.staging_dir(staging.snapshot_id) / output
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

        item.output = output
        if variant_obj.name != item.variant:
            item.resolved_variant = variant_obj.name
        item.provenance = DefinitionProvenance(
            PROVENANCE_HUB,
            self.hub_origin,
            self.project.hub_service.plugin_endpoint(
                plugin_type,
                item.name,
                variant_name=variant_obj.name,
            ),
        )
        item.is_default_variant = definition.is_default_variant
        item.is_deprecated = variant_obj.deprecated
        item.status = ITEM_SUCCESS
        item.error = None

    def _resolve_one(
        self,
        staging: Staging,
        item: StagingItem,
    ) -> str:
        """Resolve one item, returning its result status.

        Args:
            staging: The staging index.
            item: The item to resolve.

        Returns:
            One of `RESULT_REUSED`, `RESULT_LOCKED`, `RESULT_FAILED`.
        """
        if self._try_reuse(staging, item):
            return RESULT_REUSED

        try:
            self._resolve_from_hub(staging, item)
        except (MeltanoError, ValueError, KeyError, TypeError) as err:
            item.status = ITEM_FAILED
            item.error = str(err)
            logger.warning("Failed to resolve plugin", key=item.key, error=str(err))
            return RESULT_FAILED

        return RESULT_LOCKED

    def prepare(self, staging: Staging) -> list[ItemResult]:
        """Resolve every item of the batch, individually.

        Args:
            staging: The staging index.

        Returns:
            One result per item.
        """
        results: list[ItemResult] = []

        for item in staging.items.values():
            descriptor = self._descriptor(item)

            if item.kind == ENTRY_LOCKED:
                status = self._resolve_one(staging, item)
            else:
                item.status = ITEM_SUCCESS
                status = RESULT_SKIPPED

            variant = item.resolved_variant or item.variant
            results.append(
                ItemResult(
                    key=item.key,
                    descriptor=descriptor,
                    kind=item.kind,
                    status=status,
                    variant=variant,
                    error=item.error,
                ),
            )
            self._write_staging(staging)

        return results

    # --- Pre-publish checks ---

    def ensure_can_publish(self, *, update: bool) -> None:
        """Check that a new batch may be started.

        Args:
            update: Whether the caller asked to replace the current snapshot.
        """
        current = self.read_manifest()
        if current is not None and not update:
            raise SnapshotExistsError(current.snapshot_id)

    # --- Assembly ---

    @staticmethod
    def _entry_filename(*, name: str, variant: str | None) -> str:
        return f"{name}--{variant}.lock" if variant else f"{name}.lock"

    def _snapshot_relpath(
        self,
        snapshot_id: str,
        *,
        plugin_type: str,
        name: str,
        variant: str | None,
    ) -> str:
        filename = self._entry_filename(name=name, variant=variant)
        return f"plugins/snapshots/{snapshot_id}/{plugin_type}/{filename}"

    def _refresh_origin_mismatched(
        self,
        staging: Staging,
        base: LockSnapshotManifest,
    ) -> None:
        """Re-resolve base locked entries whose origin is no longer configured."""
        current_names = {(item.type, item.name) for item in staging.items.values()}
        extras: list[StagingItem] = []

        for base_entry in base.entries:
            if base_entry.kind != ENTRY_LOCKED:
                continue

            if (base_entry.type, base_entry.name) in current_names:
                continue

            provenance = base_entry.provenance
            if not (
                provenance is not None
                and provenance.kind == PROVENANCE_HUB
                and provenance.origin != self.hub_origin
            ):
                continue

            item = StagingItem(
                key=(f"{base_entry.type}/{base_entry.name}--{base_entry.variant}"),
                type=base_entry.type,
                name=base_entry.name,
                variant=base_entry.variant,
                fingerprint=compute_fingerprint(
                    plugin_type=base_entry.type,
                    plugin_name=base_entry.name,
                    variant=base_entry.variant,
                    hub_origin=self.hub_origin,
                ),
                status=ITEM_PENDING,
            )
            staging.items[item.key] = item
            extras.append(item)

        for item in extras:
            self._resolve_one(staging, item)
            self._write_staging(staging)

    def _entry_from_item(
        self,
        snapshot_id: str,
        item: StagingItem,
    ) -> SnapshotEntry:
        """Build a snapshot entry from a staging item and copy its file."""
        if item.kind == ENTRY_LOCKED:
            variant = item.resolved_variant or item.variant
            rel = self._snapshot_relpath(
                snapshot_id,
                plugin_type=item.type,
                name=item.name,
                variant=variant,
            )
            destination = self.project.root / rel
            destination.parent.mkdir(parents=True, exist_ok=True)
            assert item.output is not None  # noqa: S101
            source = self.staging_dir(snapshot_id) / item.output
            destination.write_bytes(source.read_bytes())

            return SnapshotEntry(
                type=item.type,
                name=item.name,
                kind=ENTRY_LOCKED,
                variant=variant,
                file=rel,
                provenance=item.provenance,
                is_default_variant=item.is_default_variant,
                is_deprecated=item.is_deprecated,
            )

        if item.kind == ENTRY_INHERITED:
            return SnapshotEntry(
                type=item.type,
                name=item.name,
                kind=ENTRY_INHERITED,
                variant=item.variant,
                inherit_from=item.inherit_from,
            )

        return SnapshotEntry(
            type=item.type,
            name=item.name,
            kind=ENTRY_CUSTOM,
        )

    def _carry_entry(
        self,
        snapshot_id: str,
        base_entry: SnapshotEntry,
    ) -> SnapshotEntry:
        """Build a snapshot entry by carrying a base entry and copying its file."""
        if base_entry.kind in {ENTRY_INHERITED, ENTRY_CUSTOM}:
            # Entries without files are immutable and need no adaptation.
            return base_entry

        rel = self._snapshot_relpath(
            snapshot_id,
            plugin_type=base_entry.type,
            name=base_entry.name,
            variant=base_entry.variant,
        )
        destination = self.project.root / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        assert base_entry.file is not None  # noqa: S101
        source = self.project.root / base_entry.file
        destination.write_bytes(source.read_bytes())

        return SnapshotEntry(
            type=base_entry.type,
            name=base_entry.name,
            kind=base_entry.kind,
            variant=base_entry.variant,
            file=rel,
            provenance=base_entry.provenance,
            is_default_variant=base_entry.is_default_variant,
            is_deprecated=base_entry.is_deprecated,
        )

    def _legacy_entries(
        self,
        snapshot_id: str,
        *,
        exclude: set[tuple[str, str]],
    ) -> list[SnapshotEntry]:
        """Adopt loose lock files as legacy entries (first snapshot only)."""
        plugins_root = self.project.dirs.root_dir("plugins")
        if not plugins_root.exists():
            return []

        results: list[SnapshotEntry] = []

        for type_dir in plugins_root.iterdir():
            if not type_dir.is_dir() or type_dir.name == LOCK_SNAPSHOTS_DIRNAME:
                continue

            try:
                plugin_type = PluginType(type_dir.name)
            except ValueError:
                continue

            for lock_path in sorted(type_dir.glob("*.lock")):
                stem = lock_path.name.removesuffix(".lock")
                if "--" in stem:
                    name, variant = stem.split("--", 1)
                else:
                    name, variant = stem, None

                if (plugin_type.value, name) in exclude:
                    continue

                rel = self._snapshot_relpath(
                    snapshot_id,
                    plugin_type=plugin_type.value,
                    name=name,
                    variant=variant,
                )
                destination = self.project.root / rel
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(lock_path.read_bytes())

                provenance = DefinitionProvenance(
                    PROVENANCE_LOCAL,
                    f"legacy:plugins/{plugin_type.value}/{lock_path.name}",
                )
                results.append(
                    SnapshotEntry(
                        type=plugin_type.value,
                        name=name,
                        kind=ENTRY_LEGACY,
                        variant=variant,
                        file=rel,
                        provenance=provenance,
                        is_default_variant=variant is None,
                    ),
                )

        return results

    def assemble(self, staging: Staging) -> LockSnapshotManifest:
        """Assemble a self-contained snapshot from resolved items.

        Args:
            staging: The staging index.

        Returns:
            The assembled manifest (not yet published).
        """
        base = self.read_manifest()
        if base is not None:
            self._refresh_origin_mismatched(staging, base)

        if any(item.status == ITEM_FAILED for item in staging.items.values()):
            reason = "Cannot publish snapshot: one or more plugins failed to resolve"
            raise SnapshotError(
                reason,
                "Resolve the failing plugins and re-run the command",
            )

        snapshot_id = staging.snapshot_id
        snapshot_dir = self.snapshot_dir(snapshot_id)
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        current_names = {(item.type, item.name) for item in staging.items.values()}

        entries: list[SnapshotEntry] = [
            self._entry_from_item(snapshot_id, item) for item in staging.items.values()
        ]

        if base is not None:
            entries.extend(
                self._carry_entry(snapshot_id, base_entry)
                for base_entry in base.entries
                if (base_entry.type, base_entry.name) not in current_names
            )
        else:
            entries.extend(
                self._legacy_entries(snapshot_id, exclude=current_names),
            )

        manifest = LockSnapshotManifest(
            snapshot_id=snapshot_id,
            project_version=staging.base_project_version,
            environment=(
                self.project.environment.name if self.project.environment else None
            ),
            created_at=self._now(),
            entries=entries,
        )

        # Write the manifest copy into the snapshot dir before switching the
        # pointer: until that switch, no reader can see this snapshot.
        manifest_path = snapshot_dir / "snapshot.json"
        manifest_path.write_text(
            json.dumps(manifest.canonical(), indent=2) + "\n",
        )

        staging.state = STATE_STAGED
        self._write_staging(staging)
        logger.info("Assembled lock snapshot", snapshot_id=snapshot_id)
        return manifest

    # --- Publication ---

    def publish(self, staging: Staging) -> LockSnapshotManifest:
        """Atomically publish an assembled snapshot.

        Args:
            staging: The staging index.

        Returns:
            The published manifest.
        """
        self.batch_lock_path.parent.mkdir(parents=True, exist_ok=True)
        with fasteners.InterProcessLock(self.batch_lock_path):
            manifest_path = self.snapshot_dir(staging.snapshot_id) / "snapshot.json"
            manifest = LockSnapshotManifest.parse(
                json.loads(manifest_path.read_text()),
            )

            current = self.read_manifest()
            current_id = current.snapshot_id if current else None
            if current_id != staging.base_snapshot_id:
                raise SnapshotConflictError(
                    expected_snapshot_id=staging.base_snapshot_id,
                    actual_snapshot_id=current_id,
                    project_version_changed=False,
                )

            if self.project_version() != staging.base_project_version:
                raise SnapshotConflictError(
                    expected_snapshot_id=staging.base_snapshot_id,
                    actual_snapshot_id=current_id,
                    project_version_changed=True,
                )

            staging.state = STATE_PUBLISHING
            self._write_staging(staging)

            pointer = self.pointer_path
            pointer.parent.mkdir(parents=True, exist_ok=True)
            tmp = pointer.parent / f".{pointer.name}.{os.getpid()}.tmp"
            tmp.write_text(json.dumps(manifest.canonical(), indent=2) + "\n")
            tmp.replace(pointer)
            logger.info("Published lock snapshot", snapshot_id=manifest.snapshot_id)

        self._discard_staging(staging.snapshot_id)
        self._prune_snapshots(keep=manifest.snapshot_id)
        return manifest

    # --- Cleanup ---

    def _discard_staging(self, snapshot_id: str) -> None:
        path = self.staging_dir(snapshot_id)
        if path.exists():
            shutil.rmtree(path)

    def _prune_snapshots(self, *, keep: str | None) -> None:
        root = self.project.dirs.plugin_lock_snapshots_dir()
        if not root.exists():
            return

        referenced: set[str] = set()
        if keep is not None:
            referenced.add(keep)
        for staging in self.iter_stagings():
            # A live staging references both its own assembled snapshot and
            # the snapshot it was based on.
            referenced.add(staging.snapshot_id)
            if staging.base_snapshot_id:
                referenced.add(staging.base_snapshot_id)
        for path in root.iterdir():
            if path.is_dir() and path.name not in referenced:
                shutil.rmtree(path)

    # --- Recovery ---

    def _can_finish(self, staging: Staging) -> bool:
        """Whether an interrupted staging can be finished safely."""
        if staging.state not in {STATE_STAGED, STATE_PUBLISHING}:
            return False

        if not (self.snapshot_dir(staging.snapshot_id) / "snapshot.json").exists():
            return False

        current = self.read_manifest()
        current_id = current.snapshot_id if current else None

        # Either the pointer is still at the base snapshot and the project
        # has not changed, or the pointer was already switched before the
        # interruption.
        return (
            current_id == staging.base_snapshot_id
            and self.project_version() == staging.base_project_version
        ) or current_id == staging.snapshot_id

    def recover(self) -> None:
        """Recover interrupted stagings and remove stale snapshots."""
        for staging in self.iter_stagings():
            if _pid_is_alive(staging.pid):
                logger.debug(
                    "Leaving lock staging of live process",
                    snapshot_id=staging.snapshot_id,
                    pid=staging.pid,
                )
                continue

            current = self.read_manifest()
            current_id = current.snapshot_id if current else None

            if self._can_finish(staging):
                if current_id == staging.snapshot_id:
                    # The pointer was already switched before interruption;
                    # only cleanup remains.
                    self._discard_staging(staging.snapshot_id)
                    logger.info(
                        "Completed interrupted snapshot publication",
                        snapshot_id=staging.snapshot_id,
                    )
                else:
                    logger.info(
                        "Completing interrupted lock snapshot",
                        snapshot_id=staging.snapshot_id,
                    )
                    self.publish(staging)
            else:
                logger.info(
                    "Discarding stale lock staging",
                    snapshot_id=staging.snapshot_id,
                    state=staging.state,
                )
                self._discard_staging(staging.snapshot_id)

        current = self.read_manifest()
        self._prune_snapshots(
            keep=current.snapshot_id if current else None,
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(tz=timezone.utc).isoformat()
