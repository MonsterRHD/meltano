"""Filesystem layout for a single plugin's install transaction."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

if t.TYPE_CHECKING:
    from pathlib import Path

    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

STATE_FILENAME = "install.json"
PLAN_FILENAME = "plan.json"
STAGING_DIRNAME = "staging"
RECOVERY_DIRNAME = "recovery"
LOCKS_DIRNAME = "locks"
MARKERS_DIRNAME = "markers"
BACKUP_PREFIX = "venv.backup-"

__all__ = ["InstallPaths"]


@dataclass(frozen=True)
class InstallPaths:
    """All on-disk paths involved in installing one plugin.

    Paths are derived only; constructing this class or reading its attributes
    never touches the filesystem. Use :meth:`ensure_layout` to explicitly
    create the directories needed for a real (non-dry-run) transaction.
    """

    project: Project
    plugin: ProjectPlugin

    @property
    def root(self) -> Path:
        """The plugin's transaction root: `.meltano/<type>/<plugin_dir_name>/`."""
        return self.project.dirs.plugin(self.plugin, make_dirs=False)

    @property
    def venv(self) -> Path:
        """The committed virtual environment (identical to the legacy path)."""
        return self.root / "venv"

    @property
    def state_path(self) -> Path:
        """Path to the committed state record."""
        return self.root / STATE_FILENAME

    @property
    def staging_root(self) -> Path:
        """Directory holding staging environments for all attempted plans."""
        return self.root / STAGING_DIRNAME

    def staging_dir(self, plan_id: str) -> Path:
        """Return the staging directory for the given plan."""
        return self.staging_root / plan_id

    def staging_venv(self, plan_id: str) -> Path:
        """Return the staged virtual environment path for the given plan."""
        return self.staging_dir(plan_id) / "venv"

    def plan_file(self, plan_id: str) -> Path:
        """Return the path to the serialized plan in the staging directory."""
        return self.staging_dir(plan_id) / PLAN_FILENAME

    def candidate_state(self, plan_id: str) -> Path:
        """Return the path for the candidate committed state in staging."""
        return self.staging_dir(plan_id) / STATE_FILENAME

    def marker(self, plan_id: str, name: str) -> Path:
        """Return the path of a build-progress marker."""
        return self.staging_dir(plan_id) / MARKERS_DIRNAME / name

    @property
    def recovery_root(self) -> Path:
        """Directory holding recovery records."""
        return self.root / RECOVERY_DIRNAME

    def recovery_record_path(self, record_id: str) -> Path:
        """Return the path for the recovery record with the given ID."""
        return self.recovery_root / f"{record_id}.json"

    @property
    def locks_dir(self) -> Path:
        """Directory holding this plugin's interprocess lock files."""
        return self.root / LOCKS_DIRNAME

    @property
    def commit_lock_path(self) -> Path:
        """Path of the per-plugin commit lock."""
        return self.locks_dir / "commit.lock"

    def venv_backup(self, plan_id: str) -> Path:
        """Return the backup path used while replacing the given committed plan."""
        return self.root / f"{BACKUP_PREFIX}{plan_id}"

    def ensure_layout(self) -> None:
        """Create the staging, recovery, and lock parent directories.

        Only call this for a real (non-dry-run) transaction.
        """
        for path in (self.staging_root, self.recovery_root, self.locks_dir):
            path.mkdir(parents=True, exist_ok=True)
