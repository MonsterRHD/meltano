"""Defines PluginRemoveService."""

from __future__ import annotations

import hashlib
import typing as t

import structlog

from meltano.core.error import MeltanoError
from meltano.core.plugin.error import PluginNotFoundError
from meltano.core.plugin_location_remove import (
    DbRemoveManager,
    InstallationCacheRemoveManager,
    InstallationRemoveManager,
    LockedDefinitionRemoveManager,
    MeltanoYmlRemoveManager,
    PluginLocationRemoveManager,
    PluginLocationRemoveStatus,
    PluginRemoveJournal,
    RemovePhase,
    _restore_from_backup,
    effective_plugin_dir_name,
)
from meltano.core.schedule import ELTSchedule, JobSchedule
from meltano.core.utils import noop

if t.TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from meltano.core.plugin import PluginType
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project
    from meltano.core.task_sets import TaskSets

logger = structlog.stdlib.get_logger(__name__)


class PluginReferenceKind:
    """Kinds of references that can block plugin removal."""

    PLUGIN = "plugin"
    ENVIRONMENT = "environment"
    JOB = "job"
    SCHEDULE = "schedule"


class PluginReference(t.NamedTuple):
    """A reference to a plugin from another part of the project."""

    kind: str
    name: str
    detail: str | None = None


class _RemovalManagers(t.NamedTuple):
    """All removal location managers involved in removing one plugin."""

    db: DbRemoveManager
    meltano_yml: MeltanoYmlRemoveManager
    locks: LockedDefinitionRemoveManager
    cache: InstallationCacheRemoveManager
    installation: InstallationRemoveManager


class PluginRemoveBlockedError(MeltanoError):
    """Raised when a plugin cannot be removed because it is still referenced."""

    def __init__(self, blockers: dict[str, list[PluginReference]]) -> None:
        """Initialize the error.

        Args:
            blockers: Mapping of plugin descriptor to its references.
        """
        self.blockers = blockers
        lines = []
        for descriptor, references in blockers.items():
            lines.append(f"{descriptor} is still referenced by:")
            for ref in references:
                suffix = f" ({ref.detail})" if ref.detail else ""
                lines.append(f"  - {ref.kind} '{ref.name}'{suffix}")
        reason = "\n".join(lines)
        instruction = (
            "Remove or update the references before removing the plugin, or "
            "rerun with `--dry-run` to review the removal plan"
        )
        super().__init__(reason=reason, instruction=instruction)


class StaleRemovalPlanError(MeltanoError):
    """Raised when the project changed between planning and committing removal."""

    def __init__(self, plugin: ProjectPlugin) -> None:
        """Initialize the error.

        Args:
            plugin: The plugin whose plan was stale.
        """
        descriptor = f"{plugin.type.descriptor} '{plugin.name}'"
        super().__init__(
            reason=f"{descriptor} removal plan is stale: meltano.yml changed "
            "after the removal plan was generated",
            instruction="Rerun `meltano remove` to generate a fresh plan",
        )


class PluginRemoveService:
    """Handle plugin installation removal operations."""

    def __init__(self, project: Project):
        """Construct a PluginRemoveService instance.

        Args:
            project: The Meltano project.
        """
        self.project = project

    def remove_plugins(
        self,
        plugins: Sequence[ProjectPlugin],
        plugin_status_cb: Callable[[ProjectPlugin], None] = noop,
        removal_manager_status_cb: Callable[
            [PluginLocationRemoveManager],
            None,
        ] = noop,
        blockers_cb: Callable[
            [ProjectPlugin, Sequence[PluginReference]],
            None,
        ]
        | None = None,
        *,
        dry_run: bool = False,
    ) -> tuple[int, int]:
        """Remove multiple plugins.

        Args:
            plugins: The plugins to remove.
            plugin_status_cb: A callback to call for each plugin.
            removal_manager_status_cb: A callback to call for each removal manager.
            blockers_cb: A callback to call with blocking references.
            dry_run: If True, only report the plan without changing the project.

        Returns:
            A tuple containing:
            1. The total number of removed plugins
            2. The total number of plugins attempted
        """
        blockers_cb = blockers_cb or self._default_blockers_callback
        batch = {(plugin.type, plugin.name) for plugin in plugins}
        num_plugins: int = len(plugins)
        removed_plugins: int = num_plugins
        blockers: dict[str, list[PluginReference]] = {}

        for plugin in plugins:
            plugin_status_cb(plugin)

            managers, references = self._remove_plugin(
                plugin,
                batch=batch,
                dry_run=dry_run,
            )

            if references:
                blockers_cb(plugin, references)
                blockers[
                    f"{plugin.type.descriptor} '{plugin.name}'"
                ] = list(references)
                removed_plugins -= 1
                continue

            any_unsuccessful = any(
                not (
                    manager.plugin_successful
                    or (dry_run and manager.plugin_pending)
                )
                for manager in managers
            )
            for manager in managers:
                removal_manager_status_cb(manager)

            if any_unsuccessful:
                removed_plugins -= 1

        if blockers and not dry_run:
            raise PluginRemoveBlockedError(blockers)

        return removed_plugins, num_plugins

    @staticmethod
    def _default_blockers_callback(
        plugin: ProjectPlugin,
        references: Sequence[PluginReference],
    ) -> None:
        """Log blocking references."""
        for ref in references:
            suffix = f" ({ref.detail})" if ref.detail else ""
            logger.error(
                "Cannot remove %s: still referenced by %s '%s'%s",
                f"{plugin.type.descriptor} '{plugin.name}'",
                ref.kind,
                ref.name,
                suffix,
            )

    def _remove_plugin(
        self,
        plugin: ProjectPlugin,
        *,
        batch: set[tuple[PluginType, str]],
        dry_run: bool,
    ) -> tuple[_RemovalManagers, list[PluginReference]]:
        """Plan and (unless this is a dry run) remove a single plugin."""
        journal = PluginRemoveJournal.load_if_exists(self.project, plugin)
        if journal and self._journal_is_current(journal, plugin):
            logger.info(
                "Resuming plugin removal from journal",
                plugin_type=plugin.type.value,
                plugin_name=plugin.name,
                phase=journal.phase,
            )
            return self._resume(journal, plugin, batch=batch, dry_run=dry_run), []
        if journal:
            logger.warning(
                "Discarding stale plugin removal journal: the plugin is "
                "defined in meltano.yml again",
                plugin_type=plugin.type.value,
                plugin_name=plugin.name,
            )
            journal.discard()

        plan = self._plan(plugin, batch=batch)
        if not plan["found"]:
            # Preserve the historical idempotent behavior for plugins that
            # are not defined in the project: each location independently
            # reports "not found" and nothing is journaled.
            return self._remove_undefined_plugin(plan["managers"]), []

        if dry_run:
            return plan["managers"], plan["references"]

        if plan["references"]:
            return plan["managers"], plan["references"]

        return self._commit(plan), []

    def _journal_is_current(
        self,
        journal: PluginRemoveJournal,
        plugin: ProjectPlugin,
    ) -> bool:
        """Return whether an existing journal still matches the project state.

        A journal from the commit phase is only used to finish cleanup if the
        plugin is no longer defined. If the plugin exists again (e.g. it was
        re-added after an interrupted removal), the journal is stale and a
        fresh plan must be generated instead of deleting the new artifacts.

        Args:
            journal: The persisted removal journal.
            plugin: The plugin being removed.

        Returns:
            True if the journal can be resumed, False if it is stale.
        """
        if journal.phase == journal.PLANNED:
            return True

        try:
            self.project.plugins.get_plugin(plugin, ensure_parent=False)
        except PluginNotFoundError:
            return True
        return False

    def _build_managers(self, plugin: ProjectPlugin) -> _RemovalManagers:
        """Construct the removal managers for ``plugin``."""
        project = self.project
        return _RemovalManagers(
            db=DbRemoveManager(plugin, project),
            meltano_yml=MeltanoYmlRemoveManager(plugin, project),
            locks=LockedDefinitionRemoveManager(plugin, project),
            cache=InstallationCacheRemoveManager(plugin, project),
            installation=InstallationRemoveManager(plugin, project),
        )

    def _plan(
        self,
        requested: ProjectPlugin,
        *,
        batch: set[tuple[PluginType, str]],
    ) -> dict[str, t.Any]:
        """Build a removal plan for ``requested`` without mutating anything."""
        try:
            plugin = self.project.plugins.get_plugin(requested, ensure_parent=False)
        except PluginNotFoundError:
            plugin = None

        managers = self._build_managers(plugin or requested)

        if plugin is None:
            for manager in managers:
                manager.prepare()
            return {
                "requested": requested,
                "plugin": None,
                "found": False,
                "references": [],
                "fingerprint": self._config_fingerprint(),
                "managers": managers,
                "shared": False,
                "descriptor": (
                    f"{requested.type.descriptor} '{requested.name}'"
                ),
            }

        references = self._find_references(plugin, batch=batch)
        shared = self._is_shared(plugin, batch=batch)
        # The installation directory (its cached contents and the venv) can
        # be shared with an inherited plugin; lock files and the project
        # definition are plugin-exclusive.
        managers.db.prepare()
        managers.meltano_yml.prepare()
        managers.locks.prepare()
        managers.cache.prepare(shared=shared)
        managers.installation.prepare(shared=shared)

        return {
            "requested": requested,
            "plugin": plugin,
            "found": True,
            "references": references,
            "fingerprint": self._config_fingerprint(),
            "managers": managers,
            "shared": shared,
            "descriptor": f"{plugin.type.descriptor} '{plugin.name}'",
        }

    def _find_references(
        self,
        plugin: ProjectPlugin,
        *,
        batch: set[tuple[PluginType, str]],
    ) -> list[PluginReference]:
        """Find all references to ``plugin`` in every environment view."""
        references: list[PluginReference] = []
        name = plugin.name
        meltano = self.project.meltano

        # Other plugins inheriting from this plugin
        references.extend(
            PluginReference(
                PluginReferenceKind.PLUGIN,
                other.name,
                detail=f"inherits from {name}",
            )
            for other in meltano.plugins[plugin.type]
            if other.name != name
            and (other.type, other.name) not in batch
            and other.inherit_from == name
        )

        # Plugin configuration inside every environment
        for environment in meltano.environments:
            references.extend(
                PluginReference(
                    PluginReferenceKind.ENVIRONMENT,
                    environment.name,
                    detail=f"{plugin.type.value} plugin config",
                )
                for env_plugin in environment.config.plugins.get(plugin.type, [])
                if env_plugin.name == name
            )

        # Jobs reference plugins by name, optionally with a `:command` suffix
        referencing_jobs = {
            job.name
            for job in meltano.jobs
            if self._job_references_plugin(job, name)
        }
        references.extend(
            PluginReference(PluginReferenceKind.JOB, job_name, detail="job task")
            for job_name in referencing_jobs
        )

        # Legacy ELT schedules name the extractor/loader directly; job
        # schedules block removal when they run a blocking job.
        for schedule in meltano.schedules:
            if isinstance(schedule, ELTSchedule) and (
                schedule.extractor == name or schedule.loader == name
            ):
                references.append(
                    PluginReference(
                        PluginReferenceKind.SCHEDULE,
                        schedule.name,
                        detail=(
                            f"uses extractor '{schedule.extractor}' and "
                            f"loader '{schedule.loader}'"
                        ),
                    ),
                )
            elif (
                isinstance(schedule, JobSchedule)
                and schedule.job in referencing_jobs
            ):
                references.append(
                    PluginReference(
                        PluginReferenceKind.SCHEDULE,
                        schedule.name,
                        detail=f"runs job '{schedule.job}'",
                    ),
                )

        return references

    @staticmethod
    def _job_references_plugin(job: TaskSets, plugin_name: str) -> bool:
        """Return whether a job's tasks invoke the named plugin."""
        return any(
            token == plugin_name or token.startswith(f"{plugin_name}:")
            for token in job.flat_args
        )

    def _is_shared(
        self,
        plugin: ProjectPlugin,
        *,
        batch: set[tuple[PluginType, str]],
    ) -> bool:
        """Return whether the plugin's install dir is used by another plugin."""
        dir_name = effective_plugin_dir_name(self.project, plugin)
        return any(
            other.name != plugin.name
            and (other.type, other.name) not in batch
            and effective_plugin_dir_name(self.project, other) == dir_name
            for other in self.project.meltano.plugins[plugin.type]
        )

    def _config_fingerprint(self) -> str:
        """Return a fingerprint of the current `meltano.yml` contents."""
        return hashlib.sha256(self.project.meltanofile.read_bytes()).hexdigest()

    def _remove_undefined_plugin(
        self,
        managers: _RemovalManagers,
    ) -> _RemovalManagers:
        """Remove a plugin that is not defined in meltano.yml (idempotent)."""
        for manager in managers:
            if manager.phase is RemovePhase.COMMIT:
                manager.remove()  # type: ignore[call-arg]
            else:
                ephemeral_journal = PluginRemoveJournal(
                    self.project,
                    manager.plugin,
                    persistent=False,
                )
                manager.remove(  # type: ignore[call-arg]
                    ephemeral_journal,
                )
        return managers

    def _commit(self, plan: dict[str, t.Any]) -> _RemovalManagers:
        """Commit the project definition changes, then retryably clean up."""
        plugin: ProjectPlugin = plan["plugin"]

        # Reject stale plans: if meltano.yml changed after planning, rebuild
        # the plan and re-validate before touching anything.
        if self._config_fingerprint() != plan["fingerprint"]:
            self.project.refresh()
            fresh = self._plan(
                plan["requested"],
                batch={(plan["requested"].type, plan["requested"].name)},
            )
            if not fresh["found"] or fresh["references"]:
                raise StaleRemovalPlanError(plugin)
            plan = fresh

        managers: _RemovalManagers = plan["managers"]
        journal = PluginRemoveJournal(self.project, plugin)
        journal.dir.mkdir(parents=True, exist_ok=True)
        journal.backup_dir.mkdir(parents=True, exist_ok=True)
        (journal.backup_dir / "meltano.yml").write_bytes(
            self.project.meltanofile.read_bytes(),
        )
        journal.begin(
            plan["fingerprint"],
            [str(path) for path in managers.installation.cleanup_paths],
        )

        yml_modified = False
        try:
            yml_modified = self._commit_steps(managers, journal)
        except Exception:
            self._rollback(journal, managers, yml_modified=yml_modified)
            raise

        if any(
            manager.plugin_error
            for manager in managers
            if manager.phase is RemovePhase.COMMIT
        ):
            self._rollback(journal, managers, yml_modified=yml_modified)
            return managers

        journal.set_committed()
        self._cleanup(managers.installation, journal, plugin)
        if managers.installation.plugin_shared:
            # The install directory still belongs to another plugin; give
            # back any cached files moved out of it during the commit.
            self._restore_shared_cache(journal, managers.cache)
            journal.discard()
        elif managers.installation.plugin_successful:
            journal.discard()
        else:
            logger.error(
                "Plugin configuration was removed, but cleanup did not finish. "
                "Rerun `meltano remove %s %s` to complete it",
                plugin.type.value,
                plugin.name,
            )
        return managers

    def _commit_steps(
        self,
        managers: _RemovalManagers,
        journal: PluginRemoveJournal,
    ) -> bool:
        """Run the journaled commit steps in order.

        Returns:
            Whether `meltano.yml` was modified.
        """
        managers.locks.remove(journal.backup_dir, journal)
        journal.mark_step("locks")
        if managers.locks.plugin_error:
            return False

        # Recompute sharing against the live project: another plugin removed
        # in the same batch may already be gone, while a plugin removed later
        # in the batch is still defined and may own this directory.
        cache_plugin = managers.cache.plugin
        shared_now = self._is_shared(
            cache_plugin,
            batch={(cache_plugin.type, cache_plugin.name)},
        )
        managers.cache.remove(journal.backup_dir, journal, shared=shared_now)
        journal.mark_step("cache")
        if managers.cache.plugin_error:
            return False

        managers.meltano_yml.remove()
        journal.mark_step("meltano_yml")
        if managers.meltano_yml.plugin_error:
            return False
        self.project.refresh()

        managers.db.remove()
        journal.mark_step("db")
        return "meltano_yml" in journal.steps_completed

    def _rollback(
        self,
        journal: PluginRemoveJournal,
        managers: _RemovalManagers,
        *,
        yml_modified: bool,
    ) -> None:
        """Restore everything moved/changed during the failed commit."""
        logger.warning(
            "Rolling back plugin removal after a failed commit step",
            plugin_type=journal.plugin_type,
            plugin_name=journal.plugin_name,
        )

        if yml_modified:
            backup_yml = journal.backup_dir / "meltano.yml"
            if backup_yml.exists():
                self.project.meltanofile.write_bytes(backup_yml.read_bytes())

        for entry in reversed(journal.moved):
            if entry["kind"] == "locks":
                destination = managers.locks.lockfile_dir / entry["relpath"]
            else:
                destination = managers.cache.path / entry["relpath"]
            _restore_from_backup(
                journal.backup_dir / entry["kind"],
                entry["relpath"],
                destination,
            )

        # Only refresh when the definition file was actually rewritten and
        # restored; otherwise cached services (and any mocks on them) stay
        # valid because the file never changed.
        if yml_modified:
            self.project.refresh()
        journal.discard()

    def _cleanup(
        self,
        installation: InstallationRemoveManager,
        journal: PluginRemoveJournal,
        plugin: ProjectPlugin,
    ) -> None:
        """Run the retryable post-commit cleanup."""
        # Recompute sharing against the committed project: the install dir
        # may be shared with a plugin that is still defined. The removed
        # plugin itself is excluded from the sharing check.
        shared = self._is_shared(
            plugin,
            batch={(plugin.type, plugin.name)},
        )
        installation.remove(journal, shared=shared)

    def _restore_shared_cache(
        self,
        journal: PluginRemoveJournal,
        cache_manager: InstallationCacheRemoveManager,
    ) -> None:
        """Restore cached files moved out of a directory that is still shared."""
        for relpath in journal.moved_paths("cache"):
            _restore_from_backup(
                journal.backup_dir / "cache",
                relpath,
                cache_manager.path / relpath,
            )

    def _resume(
        self,
        journal: PluginRemoveJournal,
        plugin: ProjectPlugin,
        *,
        batch: set[tuple[PluginType, str]],
        dry_run: bool,
    ) -> _RemovalManagers:
        """Converge a plugin removal from an existing journal after retry/crash."""
        if dry_run:
            # Nothing new to plan; the removal is already partially executed.
            return self._resume_statuses(journal, plugin)

        if journal.phase == journal.PLANNED:
            try:
                self.project.plugins.get_plugin(plugin, ensure_parent=False)
            except PluginNotFoundError:
                still_defined = False
            else:
                still_defined = True

            if still_defined:
                # The commit never finished: undo any moves, discard the
                # stale journal and run the removal from a fresh plan.
                managers = self._build_managers(plugin)
                self._rollback(journal, managers, yml_modified=False)
                return self._remove_plugin(
                    plugin,
                    batch=batch,
                    dry_run=False,
                )[0]

            # The definition is gone even though the journal never recorded
            # the committed phase: finish the remaining commit steps.
            managers = self._build_managers(plugin)
            self._finish_commit_steps(journal, managers)
        else:
            managers = self._build_managers(plugin)
            self._restore_committed_statuses(journal, managers)

        commit_failed = any(
            manager.plugin_error
            for manager in managers
            if manager.phase is RemovePhase.COMMIT
        )
        if not commit_failed:
            journal.set_committed()
            self._cleanup(managers.installation, journal, plugin)
            if managers.installation.plugin_shared:
                self._restore_shared_cache(journal, managers.cache)
                journal.discard()
            elif managers.installation.plugin_successful:
                journal.discard()
            else:
                logger.error(
                    "Plugin configuration was removed, but cleanup did not "
                    "finish. Rerun `meltano remove %s %s` to complete it",
                    plugin.type.value,
                    plugin.name,
                )
        return managers

    def _resume_statuses(
        self,
        journal: PluginRemoveJournal,
        plugin: ProjectPlugin,
    ) -> _RemovalManagers:
        """Report journaled progress without mutating anything (dry run)."""
        managers = self._build_managers(plugin)
        status_by_step = {
            "meltano_yml": managers.meltano_yml,
            "locks": managers.locks,
            "cache": managers.cache,
            "db": managers.db,
        }
        for step, manager in status_by_step.items():
            manager.remove_status = (
                PluginLocationRemoveStatus.REMOVED
                if step in journal.steps_completed
                else PluginLocationRemoveStatus.PENDING
            )
        managers.installation.remove_status = (
            PluginLocationRemoveStatus.PENDING
            if journal.cleanup_pending
            else PluginLocationRemoveStatus.REMOVED
        )
        return managers

    def _finish_commit_steps(
        self,
        journal: PluginRemoveJournal,
        managers: _RemovalManagers,
    ) -> None:
        """Idempotently finish commit steps after a crash mid-commit.

        The plugin definition is known to be gone already.
        """
        managers.meltano_yml.remove_status = PluginLocationRemoveStatus.REMOVED
        journal.mark_step("meltano_yml")

        for step, manager in (
            ("locks", managers.locks),
            ("cache", managers.cache),
            ("db", managers.db),
        ):
            if step in journal.steps_completed:
                manager.remove_status = PluginLocationRemoveStatus.REMOVED
                continue
            manager.remove()  # type: ignore[call-arg]
            if manager.plugin_error:
                return
            # Files may already be gone because the crash happened after the
            # move/delete but before the step was journaled.
            if manager.plugin_not_found and any(
                entry["kind"] == step for entry in journal.moved
            ):
                manager.remove_status = PluginLocationRemoveStatus.REMOVED
            journal.mark_step(step)

    def _restore_committed_statuses(
        self,
        journal: PluginRemoveJournal,
        managers: _RemovalManagers,
    ) -> None:
        """Reflect already-completed commit steps when resuming."""
        status_by_step = {
            "meltano_yml": managers.meltano_yml,
            "locks": managers.locks,
            "cache": managers.cache,
            "db": managers.db,
        }
        for step, manager in status_by_step.items():
            manager.remove_status = (
                PluginLocationRemoveStatus.REMOVED
                if step in journal.steps_completed
                else PluginLocationRemoveStatus.NOT_FOUND
            )
