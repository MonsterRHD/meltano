"""Atomic commit of a staged plugin environment."""

from __future__ import annotations

import errno
import json
import shutil
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

import fasteners
import structlog

from meltano.core.venv_service import VirtualEnv

from .errors import (
    InstallTransactionError,
    StalePlanError,
)
from .paths import InstallPaths

if t.TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project
    from .plan import InstallPlan, InstallPlanService

DEFAULT_LOCK_TIMEOUT = 60

logger = structlog.stdlib.get_logger(__name__)

__all__ = ["CommittedState", "InstallCommitter"]


@dataclass(frozen=True)
class CommittedState:
    """The committed state record tying a venv to a plan and lock view."""

    plan_id: str
    plugin_type: str
    plugin_name: str
    plugin_dir_name: str
    variant: str | None
    python: str
    pip_install_args: tuple[str, ...]
    lock_hash: str
    fingerprint: str
    committed_at: str
    meltano_version: str
    schema_version: int = 1

    def to_dict(self) -> dict[str, t.Any]:
        """Serialize the state to a JSON-compatible dictionary."""
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "plugin_type": self.plugin_type,
            "plugin_name": self.plugin_name,
            "plugin_dir_name": self.plugin_dir_name,
            "variant": self.variant,
            "python": self.python,
            "pip_install_args": list(self.pip_install_args),
            "lock_hash": self.lock_hash,
            "fingerprint": self.fingerprint,
            "committed_at": self.committed_at,
            "meltano_version": self.meltano_version,
        }

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> CommittedState:
        """Reconstruct state from a dictionary."""
        return cls(
            plan_id=data["plan_id"],
            plugin_type=data["plugin_type"],
            plugin_name=data["plugin_name"],
            plugin_dir_name=data["plugin_dir_name"],
            variant=data.get("variant"),
            python=data["python"],
            pip_install_args=tuple(data["pip_install_args"]),
            lock_hash=data["lock_hash"],
            fingerprint=data["fingerprint"],
            committed_at=data["committed_at"],
            meltano_version=data["meltano_version"],
            schema_version=data.get("schema_version", 1),
        )

    @classmethod
    def from_plan(
        cls,
        plan: InstallPlan,
        *,
        committed_at: str,
    ) -> CommittedState:
        """Build the committed state for a resolved plan."""
        return cls(
            plan_id=plan.plan_id,
            plugin_type=plan.plugin_type,
            plugin_name=plan.plugin_name,
            plugin_dir_name=plan.plugin_dir_name,
            variant=plan.variant,
            python=plan.python,
            pip_install_args=plan.pip_install_args,
            lock_hash=plan.lock_hash,
            fingerprint=plan.fingerprint_value,
            committed_at=committed_at,
            meltano_version=plan.meltano_version,
        )


class InstallCommitter:
    """Atomically swap a staged environment into the committed location."""

    def __init__(
        self,
        project: Project,
        plan_service: InstallPlanService,
        *,
        lock_timeout: int = DEFAULT_LOCK_TIMEOUT,
    ):
        """Initialize the committer.

        Args:
            project: The Meltano project.
            plan_service: Service used to re-resolve the current plan.
            lock_timeout: Max seconds to wait for the commit lock.
        """
        self.project = project
        self.plan_service = plan_service
        self.lock_timeout = lock_timeout

    def load_committed_state(
        self,
        plugin: ProjectPlugin,
    ) -> CommittedState | None:
        """Read the committed state, returning None if absent or unreadable."""
        paths = InstallPaths(self.project, plugin)
        if not paths.state_path.exists():
            return None
        try:
            return CommittedState.from_dict(json.loads(paths.state_path.read_text()))
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning(
                "Ignoring unreadable committed state record",
                path=paths.state_path,
            )
            return None

    def commit(
        self,
        plugin: ProjectPlugin,
        plan: InstallPlan,
        *,
        offline: bool = False,
    ) -> CommittedState:
        """Commit the staged environment for the plan.

        Args:
            plugin: The plugin being installed.
            plan: The staged install plan.
            offline: Re-resolve without contacting the Hub.

        Returns:
            The new committed state.

        Raises:
            StalePlanError: The plan no longer matches current project/lock.
            InstallTransactionError: The swap failed or the lock could not be
                acquired; the previous version is restored.
        """
        paths = InstallPaths(self.project, plugin)
        paths.locks_dir.mkdir(parents=True, exist_ok=True)

        with self._commit_lock(paths):
            from meltano.core.plugin_install_service import PluginInstallReason

            reason = PluginInstallReason(plan.reason)
            current = self.plan_service.resolve(plugin, reason, offline=offline)
            if current.plan_id != plan.plan_id:
                raise StalePlanError(
                    reason=(
                        f"Staged plan {plan.plan_id[:12]} is stale; the current "
                        f"plan is {current.plan_id[:12]}"
                    ),
                    instruction=(
                        "The project configuration or lockfile changed; review the"
                        " changes and retry the installation"
                    ),
                )

            staged_venv = paths.staging_venv(plan.plan_id)
            if not staged_venv.exists():
                raise InstallTransactionError(
                    reason=(
                        f"Staged environment for plan {plan.plan_id[:12]} is missing"
                    ),
                    instruction=(
                        "Retry the installation to rebuild the staged environment"
                    ),
                )

            previous = self.load_committed_state(plugin)
            backup: Path | None = None
            if paths.venv.exists():
                backup = paths.venv_backup(
                    previous.plan_id if previous else "unknown",
                )
                _move_path(paths.venv, backup)

            failure = None
            try:
                _move_path(staged_venv, paths.venv)
            except OSError as err:
                if backup:
                    _move_path(backup, paths.venv)
                raise InstallTransactionError(
                    reason="Failed to move the staged environment into place",
                    instruction=(
                        "Review filesystem permissions and retry the installation"
                    ),
                ) from err

            venv = VirtualEnv(paths.venv, python=plan.python)
            if not venv.exec_path("python").exists():
                failure = "the new environment has no Python interpreter"
            elif venv.read_fingerprint() != plan.fingerprint_value:
                failure = "the new environment fingerprint does not match the plan"

            if failure is not None:
                self._rollback(paths, backup, staged_venv)
                raise InstallTransactionError(
                    reason=f"Committed environment verification failed: {failure}",
                    instruction=(
                        "Retry the installation; the previous version was restored"
                    ),
                )

            state = CommittedState.from_plan(
                plan,
                committed_at=datetime.now(UTC).isoformat(),
            )
            try:
                _atomic_write_json(paths.state_path, state.to_dict())
            except OSError as err:
                self._rollback(paths, backup, staged_venv)
                raise InstallTransactionError(
                    reason="Failed to write the committed state record",
                    instruction=(
                        "Review filesystem permissions and retry the installation"
                    ),
                ) from err

            if backup:
                shutil.rmtree(backup, ignore_errors=True)
            shutil.rmtree(paths.staging_dir(plan.plan_id), ignore_errors=True)
            return state

    @contextmanager
    def _commit_lock(
        self,
        paths: InstallPaths,
    ) -> Iterator[None]:
        lock = fasteners.InterProcessLock(paths.commit_lock_path)
        acquired = lock.acquire(timeout=self.lock_timeout)
        if not acquired:
            raise InstallTransactionError(
                reason=(
                    "Timed out waiting for another process to finish installing this"
                    " plugin"
                ),
                instruction="Wait for the other installation to finish, then retry",
            )
        try:
            yield
        finally:
            lock.release()

    @staticmethod
    def _rollback(
        paths: InstallPaths,
        backup: Path | None,
        staged_venv: Path,
    ) -> None:
        try:
            if paths.venv.exists():
                _move_path(paths.venv, staged_venv)
            if backup and backup.exists():
                _move_path(backup, paths.venv)
        except OSError:
            logger.critical(
                "Failed to roll back plugin installation",
                path=paths.venv,
                exc_info=True,
            )


def _atomic_write_json(path: Path, payload: t.Mapping[str, t.Any]) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    tmp.replace(path)


def _move_path(src: Path, dst: Path) -> None:
    try:
        src.rename(dst)
    except OSError as err:
        if err.errno != errno.EXDEV:
            raise
        # Cross-device swap: fall back to copy/remove (non-atomic window).
        logger.warning(
            "Cross-device install swap; falling back to copy (non-atomic window)",
            src=src,
            dst=dst,
        )
        shutil.copytree(src, dst, symlinks=True)
        shutil.rmtree(src)
