"""Defines plugin removers.

Removal is split into two phases:

1. The *commit* phase updates the project definition (``meltano.yml``), the
   lock files, the plugin settings in the system database and the plugin
   cache files inside ``.meltano``. Every mutation performed during this
   phase is journaled and can be rolled back if a later step fails, so a
   failure never leaves the project with half of the plugin deleted.
2. The *cleanup* phase removes the virtual environment and other generated
   artifacts. This happens only after the commit phase succeeded, is
   idempotent and may be retried (either by rerunning the command or
   automatically after a crash) using the journal.
"""

from __future__ import annotations

import json
import shutil
import sys
import typing as t
from abc import ABC, abstractmethod
from contextlib import suppress
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import sqlalchemy.exc
import structlog

from meltano.core.db import project_engine
from meltano.core.plugin.error import PluginNotFoundError
from meltano.core.plugin.settings_service import PluginSettingsService
from meltano.core.utils import sanitize_filename

from .settings_store import SettingValueStore

if sys.version_info >= (3, 12):
    from typing import override  # noqa: ICN003
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from collections.abc import Iterator

    from meltano.core.plugin.project_plugin import ProjectPlugin

    from .project import Project

logger = structlog.stdlib.get_logger(__name__)


def effective_plugin_dir_name(project: Project, plugin: ProjectPlugin) -> str:
    """Resolve the installation directory name used by ``plugin``.

    Mirrors :attr:`ProjectPlugin.plugin_dir_name` using only project-local
    definitions, avoiding parent/hub resolution. Inherited plugins with the
    same (or no) ``pip_url`` reuse the parent's directory; chains ending in a
    lockfile/hub parent resolve to the ``inherit_from`` name.

    Args:
        project: The Meltano project.
        plugin: The plugin to resolve.

    Returns:
        The effective directory name under `.meltano/<plugin type>/`.
    """
    current = plugin
    seen: set[tuple[str, str]] = set()
    while current.inherit_from:
        key = (current.name, current.inherit_from)
        if key in seen:
            break
        seen.add(key)
        parent = next(
            (
                candidate
                for candidate in project.meltano.plugins[current.type]
                if candidate.name == current.inherit_from
            ),
            None,
        )
        if parent is None:
            return current.inherit_from
        if current.pip_url and parent.pip_url != current.pip_url:
            return current.name
        current = parent
    return current.name


class PluginLocationRemoveStatus(Enum):
    """Possible remove statuses."""

    REMOVED = "removed"
    ERROR = "error"
    NOT_FOUND = "not found"
    PENDING = "pending"
    SHARED = "shared"


class RemovePhase(Enum):
    """The phase a removal location belongs to."""

    COMMIT = "commit"
    CLEANUP = "cleanup"


class PluginLocationRemoveManager(ABC):
    """Handle removal of a plugin from a given location."""

    phase: t.ClassVar[RemovePhase] = RemovePhase.COMMIT

    def __init__(self, plugin: ProjectPlugin, location: str) -> None:
        """Construct a PluginLocationRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            location: The location to remove the plugin from.
        """
        self.plugin = plugin
        self.plugin_descriptor = f"{plugin.type.descriptor} '{plugin.name}'"
        self.location = location
        self.remove_status: PluginLocationRemoveStatus | None = None
        self.message: str | None = None

    @abstractmethod
    def prepare(self, *, shared: bool = False) -> None:
        """Determine the expected outcome of removing this location.

        This must not mutate the filesystem. It sets ``remove_status`` to
        one of ``PENDING``, ``NOT_FOUND`` or ``SHARED``.

        Args:
            shared: Whether the artifact backing this location is shared
                with another plugin that is not being removed.
        """

    @property
    def plugin_removed(self) -> bool:
        """Whether or not the plugin was successfully removed.

        Returns:
            True if the plugin was successfully removed, False otherwise.
        """
        return self.remove_status is PluginLocationRemoveStatus.REMOVED

    @property
    def plugin_not_found(self) -> bool:
        """Whether or not the plugin was not found to remove.

        Returns:
            True if the plugin was not found, False otherwise.
        """
        return self.remove_status is PluginLocationRemoveStatus.NOT_FOUND

    @property
    def plugin_error(self) -> bool:
        """Whether or not an error was encountered the plugin removal process.

        Returns:
            True if an error was encountered, False otherwise.
        """
        return self.remove_status is PluginLocationRemoveStatus.ERROR

    @property
    def plugin_pending(self) -> bool:
        """Whether or not removal was planned but not executed.

        Returns:
            True if removal is only planned (e.g. a dry run), False otherwise.
        """
        return self.remove_status is PluginLocationRemoveStatus.PENDING

    @property
    def plugin_shared(self) -> bool:
        """Whether or not the artifact was kept because it was shared.

        Returns:
            True if the artifact was shared and therefore kept, False otherwise.
        """
        return self.remove_status is PluginLocationRemoveStatus.SHARED

    @property
    def plugin_successful(self) -> bool:
        """Whether or not this location reached its desired end state.

        A shared artifact being intentionally kept counts as success, but a
        missing artifact does not (removing a plugin that was never fully
        installed is reported as a partial removal).

        Returns:
            True if the location was removed or intentionally shared.
        """
        return self.remove_status in (
            PluginLocationRemoveStatus.REMOVED,
            PluginLocationRemoveStatus.SHARED,
        )


class PluginRemoveJournal:
    """A durable record of a plugin removal used for crash recovery.

    The journal lives at
    ``.meltano/run/plugin-remove/<plugin-type>--<plugin-name>/journal.json``
    and tracks the completed commit steps, the files moved into the backup
    directory (used for rollback) and the pending cleanup paths.
    """

    PLANNED = "planned"
    COMMITTED = "committed"

    def __init__(
        self,
        project: Project,
        plugin: ProjectPlugin,
        *,
        persistent: bool = True,
    ) -> None:
        """Construct a PluginRemoveJournal instance.

        Args:
            project: The Meltano project.
            plugin: The plugin being removed.
            persistent: Whether the journal should be written to disk. An
                ephemeral (non-persistent) journal is used for the legacy,
                already-idempotent removal of plugins that are not defined.
        """
        self.project = project
        self.persistent = persistent
        key = (
            f"{sanitize_filename(plugin.type.value)}--{sanitize_filename(plugin.name)}"
        )
        self.dir = project.dirs.run("plugin-remove", key, make_dirs=False)
        self.path = self.dir / "journal.json"
        self.backup_dir = self.dir / "backup"
        self.plugin_type = plugin.type.value
        self.plugin_name = plugin.name
        self.phase: str = self.PLANNED
        self.fingerprint: str | None = None
        self.steps_completed: list[str] = []
        self.moved: list[dict[str, str]] = []
        self.cleanup_pending: list[str] = []
        self.cleanup_done: list[str] = []
        self.created_at: str | None = None
        self.updated_at: str | None = None

    @classmethod
    def load_if_exists(
        cls,
        project: Project,
        plugin: ProjectPlugin,
    ) -> PluginRemoveJournal | None:
        """Load an existing journal for ``plugin``, if any.

        Args:
            project: The Meltano project.
            plugin: The plugin being removed.

        Returns:
            The loaded journal, or None if there is no journal.
        """
        journal = cls(project, plugin)
        if not journal.path.exists():
            return None

        try:
            data = json.loads(journal.path.read_text())
        except (OSError, json.JSONDecodeError):
            logger.warning(
                "Could not read plugin removal journal, ignoring it",
                path=str(journal.path),
                exc_info=True,
            )
            return None

        journal.phase = data.get("phase", cls.PLANNED)
        journal.fingerprint = data.get("fingerprint")
        journal.steps_completed = list(data.get("steps_completed", []))
        journal.moved = list(data.get("moved", []))
        journal.cleanup_pending = list(data.get("cleanup_pending", []))
        journal.cleanup_done = list(data.get("cleanup_done", []))
        journal.created_at = data.get("created_at")
        journal.updated_at = data.get("updated_at")
        return journal

    def exists(self) -> bool:
        """Return whether the journal file exists."""
        return self.path.exists()

    def as_dict(self) -> dict[str, t.Any]:
        """Return the journal data as a JSON-serializable dictionary."""
        return {
            "plugin_type": self.plugin_type,
            "plugin_name": self.plugin_name,
            "phase": self.phase,
            "fingerprint": self.fingerprint,
            "steps_completed": self.steps_completed,
            "moved": self.moved,
            "cleanup_pending": self.cleanup_pending,
            "cleanup_done": self.cleanup_done,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def save(self) -> None:
        """Persist the journal to disk.

        The write goes through a temporary file so a crash cannot leave a
        truncated journal behind.
        """
        if not self.persistent:
            return
        self.updated_at = datetime.now(timezone.utc).isoformat()
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(self.as_dict(), indent=True) + "\n")
        tmp_path.replace(self.path)

    def begin(self, fingerprint: str, cleanup_paths: t.Iterable[str]) -> None:
        """Initialize and persist a fresh journal.

        Args:
            fingerprint: Fingerprint of ``meltano.yml`` at planning time.
            cleanup_paths: Artifact paths to remove during the cleanup phase.
        """
        now = datetime.now(timezone.utc).isoformat()
        self.created_at = now
        self.updated_at = now
        self.phase = self.PLANNED
        self.fingerprint = fingerprint
        self.steps_completed = []
        self.moved = []
        self.cleanup_pending = [str(path) for path in cleanup_paths]
        self.cleanup_done = []
        self.dir.mkdir(parents=True, exist_ok=True)
        self.save()

    def mark_step(self, step: str) -> None:
        """Record a commit step as completed and persist the journal."""
        if step not in self.steps_completed:
            self.steps_completed.append(step)
        self.save()

    def record_move(self, kind: str, relpath: str) -> None:
        """Record a path that was moved into the backup directory."""
        entry = {"kind": kind, "relpath": relpath}
        if entry not in self.moved:
            self.moved.append(entry)
        self.save()

    def set_committed(self) -> None:
        """Mark the commit phase as completed."""
        self.phase = self.COMMITTED
        self.save()

    def mark_cleaned(self, path: str) -> None:
        """Move a cleanup path from pending to done."""
        if path in self.cleanup_pending:
            self.cleanup_pending.remove(path)
        if path not in self.cleanup_done:
            self.cleanup_done.append(path)
        self.save()

    def drop_cleanup_path(self, path: str) -> None:
        """Stop tracking a cleanup path (e.g. because it is shared)."""
        if path in self.cleanup_pending:
            self.cleanup_pending.remove(path)
        if path not in self.cleanup_done:
            self.cleanup_done.append(path)
        self.save()

    def discard(self) -> None:
        """Remove the journal and its backups after a successful removal."""
        if not self.persistent:
            return
        shutil.rmtree(self.dir, ignore_errors=True)
        # Remove the shared parent directory too when no other removals are
        # being tracked, ignoring failure if other journals still live there.
        with suppress(OSError):
            self.dir.parent.rmdir()

    def moved_paths(self, kind: str) -> Iterator[str]:
        """Yield relative paths moved into ``backup/<kind>``."""
        for entry in self.moved:
            if entry["kind"] == kind:
                yield entry["relpath"]


def _move_to_backup(path: Path, backup_root: Path, relpath: str) -> None:
    """Move ``path`` into ``backup_root`` preserving ``relpath``.

    Args:
        path: The path to move.
        backup_root: Directory to move the path into.
        relpath: Relative location to preserve inside ``backup_root``.
    """
    destination = backup_root / relpath
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(destination))


def _restore_from_backup(backup_root: Path, relpath: str, destination: Path) -> None:
    """Restore a previously backed up path.

    Args:
        backup_root: Directory holding the backup.
        relpath: Location of the file inside ``backup_root``.
        destination: Where to restore the file to.
    """
    source = backup_root / relpath
    if not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))


class DbRemoveManager(PluginLocationRemoveManager):
    """Handle removal from the system db `plugin_settings` table."""

    phase = RemovePhase.COMMIT

    def __init__(self, plugin: ProjectPlugin, project: Project) -> None:
        """Construct a DbRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            project: The Meltano project.
        """
        super().__init__(plugin, "system database")
        self.plugins_settings_service = PluginSettingsService(project, plugin)
        self.session = project_engine(project)[1]

    @override
    def prepare(self, *, shared: bool = False) -> None:
        """Resetting plugin settings is always applicable."""
        self.remove_status = PluginLocationRemoveStatus.PENDING

    def remove(self) -> None:
        """Remove the plugin's settings from the system db `plugin_settings` table."""
        session = self.session()
        try:
            self.plugins_settings_service.reset(
                store=SettingValueStore.DB,
                session=session,
            )
        except sqlalchemy.exc.OperationalError as err:
            session.rollback()
            self.remove_status = PluginLocationRemoveStatus.ERROR
            self.message = str(err.orig) if err.orig else str(err)
            return
        finally:
            session.close()

        self.remove_status = PluginLocationRemoveStatus.REMOVED


class MeltanoYmlRemoveManager(PluginLocationRemoveManager):
    """Handle removal of a plugin from `meltano.yml`."""

    phase = RemovePhase.COMMIT

    def __init__(self, plugin: ProjectPlugin, project: Project) -> None:
        """Construct a MeltanoYmlRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            project: The Meltano project.
        """
        super().__init__(plugin, str(project.meltanofile.relative_to(project.root)))
        self.project = project

    @override
    def prepare(self, *, shared: bool = False) -> None:
        """Check whether the plugin is defined in `meltano.yml`."""
        try:
            self.project.plugins.get_plugin(self.plugin, ensure_parent=False)
        except PluginNotFoundError:
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
        except OSError:
            self.remove_status = PluginLocationRemoveStatus.PENDING
        else:
            self.remove_status = PluginLocationRemoveStatus.PENDING

    def remove(self) -> None:
        """Remove the plugin from `meltano.yml`."""
        try:
            self.project.plugins.remove_from_file(self.plugin)
        except PluginNotFoundError:
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
            return
        except OSError as err:
            self.remove_status = PluginLocationRemoveStatus.ERROR
            self.message = err.strerror
            return

        self.remove_status = PluginLocationRemoveStatus.REMOVED


class LockedDefinitionRemoveManager(PluginLocationRemoveManager):
    """Handle removal of a plugin locked definition from `plugins/`."""

    phase = RemovePhase.COMMIT

    def __init__(self, plugin: ProjectPlugin, project: Project) -> None:
        """Construct a LockedDefinitionRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            project: The Meltano project.
        """
        self.lockfile_dir = project.dirs.root_plugins(plugin.type, make_dirs=False)
        glob_expr = f"{plugin.name}*.lock"
        super().__init__(
            plugin,
            str(self.lockfile_dir.relative_to(project.root).joinpath(glob_expr)),
        )
        # Match `<name>.lock` and `<name>--<variant>.lock` exactly; a loose
        # `<name>*.lock` glob would also match a different plugin whose name
        # happens to start with the same string (e.g. `tap` vs `tap-gitlab`).
        self.paths = [
            path
            for pattern in (f"{plugin.name}.lock", f"{plugin.name}--*.lock")
            for path in self.lockfile_dir.glob(pattern)
        ]

    @override
    def prepare(self, *, shared: bool = False) -> None:
        """Check whether lock files exist for the plugin."""
        self.remove_status = (
            PluginLocationRemoveStatus.NOT_FOUND
            if not self.paths
            else PluginLocationRemoveStatus.PENDING
        )

    def remove(
        self,
        backup_dir: Path | None = None,
        journal: PluginRemoveJournal | None = None,
    ) -> None:
        """Remove the plugin lock files from `plugins/`.

        Args:
            backup_dir: If provided, move the lock files here instead of
                deleting them so the removal can be rolled back.
            journal: Journal recording moved files, for rollback/recovery.
        """
        if not self.paths:
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
            return

        locks_backup = backup_dir / "locks" if backup_dir else None
        try:
            for path in self.paths:
                relpath = path.name
                if locks_backup:
                    _move_to_backup(path, locks_backup, relpath)
                    if journal:
                        journal.record_move("locks", relpath)
                else:
                    path.unlink()
        except OSError as err:
            self.remove_status = PluginLocationRemoveStatus.ERROR
            self.message = err.strerror
            return

        self.remove_status = PluginLocationRemoveStatus.REMOVED


class InstallationCacheRemoveManager(PluginLocationRemoveManager):
    """Handle removal of plugin cache files from its `.meltano` directory.

    Only the generated/cached contents of the plugin installation directory
    are removed here. The virtual environment itself is removed later, during
    the retryable cleanup phase, and shared installation directories are left
    untouched.
    """

    phase = RemovePhase.COMMIT

    def __init__(self, plugin: ProjectPlugin, project: Project) -> None:
        """Construct an InstallationCacheRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            project: The Meltano project.
        """
        self.path = project.dirs.meltano(
            plugin.type,
            effective_plugin_dir_name(project, plugin),
            make_dirs=False,
        )
        super().__init__(plugin, str(self.path.parent.relative_to(project.root)))
        self.shared = False

    def _children(self) -> list[Path]:
        """Return the cached (non-venv) children of the installation directory."""
        if not self.path.exists():
            return []
        return [child for child in self.path.iterdir() if child.name != "venv"]

    @override
    def prepare(self, *, shared: bool = False) -> None:
        """Record whether cache files exist and whether the directory is shared."""
        self.shared = shared
        if shared:
            self.remove_status = PluginLocationRemoveStatus.SHARED
        elif not self.path.exists():
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
        else:
            self.remove_status = PluginLocationRemoveStatus.PENDING

    def remove(
        self,
        backup_dir: Path | None = None,
        journal: PluginRemoveJournal | None = None,
        *,
        shared: bool = False,
    ) -> None:
        """Move the plugin cache files into the backup directory.

        Args:
            backup_dir: Directory to move cache files into. If None, the
                files are deleted directly instead (no rollback possible).
            journal: Journal recording moved files, for rollback/recovery.
            shared: Whether the installation directory is shared with a
                plugin that is not being removed.
        """
        self.shared = shared
        if shared:
            self.remove_status = PluginLocationRemoveStatus.SHARED
            return

        if not self.path.exists():
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
            return

        cache_backup = backup_dir / "cache" if backup_dir else None
        try:
            for child in self._children():
                relpath = child.relative_to(self.path).as_posix()
                if cache_backup and journal:
                    _move_to_backup(child, cache_backup, relpath)
                    journal.record_move("cache", relpath)
                elif child.is_dir() and not child.is_symlink():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        except OSError as err:
            self.remove_status = PluginLocationRemoveStatus.ERROR
            self.message = err.strerror
            return

        self.remove_status = PluginLocationRemoveStatus.REMOVED


class InstallationRemoveManager(PluginLocationRemoveManager):
    """Handle removal of a plugin installation from `.meltano`."""

    phase = RemovePhase.CLEANUP

    def __init__(self, plugin: ProjectPlugin, project: Project) -> None:
        """Construct a InstallationRemoveManager instance.

        Args:
            plugin: The plugin to remove.
            project: The Meltano project.
        """
        self.path = project.dirs.meltano(
            plugin.type,
            effective_plugin_dir_name(project, plugin),
            make_dirs=False,
        )
        self.venv_path = self.path / "venv"
        self.run_path = project.dirs.run(plugin.name, make_dirs=False)
        super().__init__(plugin, str(self.path.parent.relative_to(project.root)))

    @property
    def cleanup_paths(self) -> list[Path]:
        """Paths removed during the cleanup phase, in removal order."""
        return [self.venv_path, self.run_path, self.path]

    @override
    def prepare(self, *, shared: bool = False) -> None:
        """Record whether the installation exists and whether it is shared."""
        if shared:
            self.remove_status = PluginLocationRemoveStatus.SHARED
        elif any(path.exists() for path in self.cleanup_paths):
            self.remove_status = PluginLocationRemoveStatus.PENDING
        else:
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
            self.message = f"{self.plugin_descriptor} not found in {self.path.parent}"

    def remove(
        self,
        journal: PluginRemoveJournal,
        *,
        shared: bool = False,
    ) -> None:
        """Remove the virtual environment and generated plugin files.

        Each path is checked off in ``journal`` as soon as it is removed, so
        an interrupted or failed cleanup can be resumed by rerunning the
        same remove operation.

        Args:
            journal: The removal journal tracking pending cleanup paths.
            shared: Whether the installation directory is shared with a
                plugin that is not being removed. Shared directories are
                left in place.
        """
        if shared:
            for path in self.cleanup_paths:
                journal.drop_cleanup_path(str(path))
            self.remove_status = PluginLocationRemoveStatus.SHARED
            return

        if not journal.cleanup_pending:
            journal.cleanup_pending = [str(path) for path in self.cleanup_paths]

        removed_anything = False
        for path_str in list(journal.cleanup_pending):
            path = Path(path_str)
            if not path.exists():
                journal.mark_cleaned(path_str)
                continue

            try:
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            except OSError as err:
                self.remove_status = PluginLocationRemoveStatus.ERROR
                self.message = err.strerror
                logger.error(  # noqa: TRY400
                    "Failed to remove plugin artifact during cleanup; rerun "
                    "`meltano remove` to retry",
                    plugin=self.plugin_descriptor,
                    path=path_str,
                )
                return

            journal.mark_cleaned(path_str)
            removed_anything = True

        if removed_anything:
            self.remove_status = PluginLocationRemoveStatus.REMOVED
        else:
            self.remove_status = PluginLocationRemoveStatus.NOT_FOUND
            self.message = f"{self.plugin_descriptor} not found in {self.path.parent}"
