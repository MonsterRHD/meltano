"""Recovery records and cross-restart reconciliation."""

from __future__ import annotations

import json
import os
import shutil
import typing as t
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from .paths import BACKUP_PREFIX, InstallPaths
from .plan import InstallPlan

if t.TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

STAGE_RESOLVE = "resolve"
STAGE_STAGING = "staging"
STAGE_CHECK = "check"
STAGE_COMMIT = "commit"

STATUS_OPEN = "open"
STATUS_SUPERSEDED = "superseded"

RECONCILE_NONE = "none"
RECONCILE_REUSABLE = "reusable"
RECONCILE_RESUMABLE = "resumable"

logger = structlog.stdlib.get_logger(__name__)

__all__ = [
    "ReconcileResult",
    "RecoveryRecord",
    "RecoveryService",
]


@dataclass(frozen=True)
class RecoveryRecord:
    """An identifiable record of a failed installation attempt."""

    record_id: str
    plan_id: str
    plugin_type: str
    plugin_name: str
    plugin_dir_name: str
    stage: str
    error: str
    timestamp: str
    staging_path: str
    previous_plan_id: str | None
    status: str
    suggestion: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, t.Any]:
        """Serialize the record to a JSON-compatible dictionary."""
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "plan_id": self.plan_id,
            "plugin_type": self.plugin_type,
            "plugin_name": self.plugin_name,
            "plugin_dir_name": self.plugin_dir_name,
            "stage": self.stage,
            "error": self.error,
            "timestamp": self.timestamp,
            "staging_path": self.staging_path,
            "previous_plan_id": self.previous_plan_id,
            "status": self.status,
            "suggestion": self.suggestion,
        }

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> RecoveryRecord:
        """Reconstruct a record from a dictionary."""
        return cls(
            record_id=data["record_id"],
            plan_id=data["plan_id"],
            plugin_type=data["plugin_type"],
            plugin_name=data["plugin_name"],
            plugin_dir_name=data["plugin_dir_name"],
            stage=data["stage"],
            error=data["error"],
            timestamp=data["timestamp"],
            staging_path=data["staging_path"],
            previous_plan_id=data.get("previous_plan_id"),
            status=data.get("status", STATUS_OPEN),
            suggestion=data.get("suggestion", ""),
            schema_version=data.get("schema_version", 1),
        )


@dataclass(frozen=True)
class ReconcileResult:
    """The outcome of reconciling staging and interrupted swaps."""

    mode: str
    open_records: tuple[RecoveryRecord, ...]


class RecoveryService:
    """Write recovery records and reconcile state across process restarts."""

    def __init__(self, project: Project):
        """Initialize the recovery service.

        Args:
            project: The Meltano project.
        """
        self.project = project

    def record_failure(
        self,
        plugin: ProjectPlugin,
        plan: InstallPlan,
        *,
        stage: str,
        error: str,
        previous_plan_id: str | None = None,
        suggestion: str | None = None,
    ) -> RecoveryRecord:
        """Persist a recovery record for a failed install attempt.

        Args:
            plugin: The plugin being installed.
            plan: The failed install plan.
            stage: The stage at which the failure occurred.
            error: A description of the failure.
            previous_plan_id: The plan ID of the previous runnable version.
            suggestion: Suggested next steps for the user.

        Returns:
            The persisted recovery record.
        """
        paths = InstallPaths(self.project, plugin)
        now = datetime.now(UTC)
        record_id = f"{plan.plan_id[:12]}-{now:%Y%m%dT%H%M%S%f}"
        record = RecoveryRecord(
            record_id=record_id,
            plan_id=plan.plan_id,
            plugin_type=plan.plugin_type,
            plugin_name=plan.plugin_name,
            plugin_dir_name=plan.plugin_dir_name,
            stage=stage,
            error=error,
            timestamp=now.isoformat(),
            staging_path=str(paths.staging_dir(plan.plan_id)),
            previous_plan_id=previous_plan_id,
            status=STATUS_OPEN,
            suggestion=suggestion
            or (
                "Fix the reported issue and retry; the previous version remains"
                " available"
            ),
        )

        target = paths.recovery_record_path(record_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(target, record.to_dict())
        logger.error(
            "Install failure recorded; previous version preserved",
            recovery_record=str(target),
            plan_id=plan.plan_id,
            stage=stage,
        )
        return record

    def iter_records(
        self,
        plugin: ProjectPlugin,
    ) -> Iterator[RecoveryRecord]:
        """Yield all recovery records for the plugin, newest first."""
        paths = InstallPaths(self.project, plugin)
        if not paths.recovery_root.exists():
            return
        records: list[RecoveryRecord] = []
        for path in paths.recovery_root.glob("*.json"):
            if record := _load_record(path):
                records.append(record)
        records.sort(key=lambda record: record.timestamp, reverse=True)
        yield from records

    def list_open(self, plugin: ProjectPlugin) -> list[RecoveryRecord]:
        """Return open recovery records for the plugin."""
        return [
            record
            for record in self.iter_records(plugin)
            if record.status == STATUS_OPEN
        ]

    def supersede_for(self, plugin: ProjectPlugin) -> int:
        """Mark open records for the plugin as superseded after success.

        Records are retained (not deleted) to preserve the recovery history.

        Args:
            plugin: The plugin that was successfully installed.

        Returns:
            The number of records updated.
        """
        paths = InstallPaths(self.project, plugin)
        updated = 0
        for record in self.iter_records(plugin):
            if record.status != STATUS_OPEN:
                continue
            superseded = RecoveryRecord(
                **{**record.to_dict(), "status": STATUS_SUPERSEDED},
            )
            target = paths.recovery_record_path(record.record_id)
            _atomic_write_json(target, superseded.to_dict())
            updated += 1
        return updated

    def reconcile(
        self,
        plugin: ProjectPlugin,
        current_plan: InstallPlan,
    ) -> ReconcileResult:
        """Repair interrupted swaps and reconcile staging directories.

        Args:
            plugin: The plugin being installed.
            current_plan: The plan resolved from the current project/lock.

        Returns:
            The reconciliation result.
        """
        paths = InstallPaths(self.project, plugin)
        self._repair_swap(paths)
        mode = self._reconcile_staging(paths, current_plan)
        open_records = tuple(self.list_open(plugin))
        for record in open_records:
            logger.info(
                "Open install recovery record",
                recovery_record=paths.recovery_record_path(record.record_id),
                plan_id=record.plan_id,
                stage=record.stage,
            )
        return ReconcileResult(mode=mode, open_records=open_records)

    @staticmethod
    def _repair_swap(paths: InstallPaths) -> None:
        backups = sorted(
            paths.root.glob(f"{BACKUP_PREFIX}*"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not backups:
            return

        if not paths.venv.exists():
            # The swap was interrupted before the new venv landed: restore
            # the newest backup and discard any older backups.
            newest, *others = backups
            os.rename(newest, paths.venv)
            for backup in others:
                shutil.rmtree(backup, ignore_errors=True)
            logger.warning(
                "Restored previous plugin environment after an interrupted swap",
                path=paths.venv,
            )
        else:
            # The new venv is in place: the committed backup is stale.
            for backup in backups:
                shutil.rmtree(backup, ignore_errors=True)

    @staticmethod
    def _reconcile_staging(
        paths: InstallPaths,
        current_plan: InstallPlan,
    ) -> str:
        if not paths.staging_root.exists():
            return RECONCILE_NONE

        mode = RECONCILE_NONE
        for staged in paths.staging_root.iterdir():
            if not staged.is_dir():
                continue
            try:
                staged_plan = InstallPlan.from_dict(
                    json.loads(paths.plan_file(staged.name).read_text()),
                )
            except (OSError, json.JSONDecodeError, KeyError):
                shutil.rmtree(staged, ignore_errors=True)
                continue

            if staged_plan.plan_id != current_plan.plan_id:
                # Staging belongs to an older/different plan: discard it.
                shutil.rmtree(staged, ignore_errors=True)
                continue

            from meltano.core.venv_service import VirtualEnv

            venv = VirtualEnv(
                paths.staging_venv(staged_plan.plan_id),
                python=staged_plan.python,
            )
            complete = (
                venv.read_fingerprint() == staged_plan.fingerprint_value
                and paths.marker(staged_plan.plan_id, "checked").exists()
            )
            mode = RECONCILE_REUSABLE if complete else RECONCILE_RESUMABLE

        return mode


def _atomic_write_json(path: Path, payload: t.Mapping[str, t.Any]) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, path)
