from __future__ import annotations

import errno
import json
import os
import shutil
import typing as t
from unittest import mock

import pytest
import yaml
from sqlalchemy.exc import OperationalError

from meltano.core.plugin import PluginType
from meltano.core.plugin.project_plugin import ProjectPlugin
from meltano.core.plugin_location_remove import (
    InstallationRemoveManager,
    PluginLocationRemoveStatus,
    PluginRemoveJournal,
)
from meltano.core.plugin_remove_service import (
    PluginRemoveBlockedError,
    PluginRemoveService,
    StaleRemovalPlanError,
)

if t.TYPE_CHECKING:
    import sys

    from meltano.core.plugin_location_remove import PluginLocationRemoveManager

    if sys.version_info >= (3, 13):
        from collections.abc import Generator
    else:
        from typing_extensions import Generator


TAP_GITLAB = {
    "name": "tap-gitlab",
    "variant": "meltanolabs",
    "pip_url": "git+https://github.com/MeltanoLabs/tap-gitlab.git",
}
TARGET_CSV = {
    "name": "target-csv",
    "variant": "meltanolabs",
    "pip_url": "git+https://github.com/MeltanoLabs/target-csv.git",
}


class TestPluginRemoveService:
    @pytest.fixture
    def subject(self, project):
        return PluginRemoveService(project)

    @pytest.fixture
    def add(self, subject: PluginRemoveService) -> Generator[None]:
        with subject.project.meltanofile.open("r") as meltano_yml:
            original = yaml.safe_load(meltano_yml)

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(
                yaml.dump(
                    {
                        "plugins": {
                            "extractors": [
                                {
                                    "name": "tap-gitlab",
                                    "variant": "meltanolabs",
                                    "pip_url": "git+https://github.com/MeltanoLabs/tap-gitlab.git",
                                },
                            ],
                            "loaders": [
                                {
                                    "name": "target-csv",
                                    "variant": "meltanolabs",
                                    "pip_url": "git+https://github.com/MeltanoLabs/target-csv.git",
                                },
                            ],
                        },
                    },
                ),
            )

        yield

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(yaml.dump(original))

    @pytest.fixture
    def no_plugins(self, subject: PluginRemoveService) -> Generator[None]:
        with subject.project.meltanofile.open("r") as meltano_yml:
            original = yaml.safe_load(meltano_yml)

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(yaml.dump({"plugins": {}}))

        yield

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(yaml.dump(original))

    @pytest.fixture
    def install(self, subject: PluginRemoveService) -> Generator[None]:
        tap_gitlab_installation = subject.project.dirs.meltano().joinpath(
            "extractors",
            "tap-gitlab",
        )
        target_csv_installation = subject.project.dirs.meltano().joinpath(
            "loaders",
            "target-csv",
        )
        tap_gitlab_installation.mkdir(parents=True, exist_ok=True)
        target_csv_installation.mkdir(parents=True, exist_ok=True)
        yield
        shutil.rmtree(tap_gitlab_installation, ignore_errors=True)
        shutil.rmtree(target_csv_installation, ignore_errors=True)

    @pytest.fixture
    def lock(self, subject: PluginRemoveService) -> Generator[None]:
        tap_gitlab_lockfile = subject.project.dirs.plugin_lock_path(
            "extractors",
            "tap-gitlab",
            variant_name="meltanolabs",
        )
        target_csv_lockfile = subject.project.dirs.plugin_lock_path(
            "loaders",
            "target-csv",
            variant_name="meltanolabs",
        )
        with tap_gitlab_lockfile.open("w") as f:
            json.dump(
                {
                    "plugin_type": "extractors",
                    "name": "tap-gitlab",
                    "namespace": "tap_gitlab",
                    "variant": "meltanolabs",
                },
                f,
            )

        with target_csv_lockfile.open("w") as f:
            json.dump(
                {
                    "plugin_type": "loaders",
                    "name": "target-csv",
                    "namespace": "target_csv",
                    "variant": "meltanolabs",
                },
                f,
            )
        yield
        tap_gitlab_lockfile.unlink(missing_ok=True)
        target_csv_lockfile.unlink(missing_ok=True)

    def test_default_init_should_not_fail(self, subject) -> None:
        assert subject

    @pytest.mark.usefixtures("add", "install", "lock")
    def test_remove(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()
        plugins = list(subject.project.plugins.plugins())
        removed_plugins, total_plugins = subject.remove_plugins(plugins)

        assert removed_plugins == total_plugins

        for plugin in plugins:
            # check removed from meltano.yml
            with subject.project.meltanofile.open() as meltanofile:
                meltano_yml = yaml.safe_load(meltanofile)

                with pytest.raises(KeyError):
                    meltano_yml[plugin.type, plugin.name]

            # check removed installation
            path = subject.project.dirs.meltano().joinpath(plugin.type, plugin.name)
            assert not path.exists()

            # check removed lock files
            lock_file_paths = list(
                subject.project.dirs.root_plugins(plugin.type).glob(
                    f"{plugin.name}*.lock",
                ),
            )
            assert all(not path.exists() for path in lock_file_paths)

    @pytest.mark.usefixtures("no_plugins")
    def test_remove_not_added_or_installed(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()
        plugins = list(subject.project.plugins.plugins())
        removed_plugins, total_plugins = subject.remove_plugins(plugins)

        assert removed_plugins == total_plugins == 0

    @pytest.mark.usefixtures("add", "install", "lock")
    def test_remove_db_error(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()
        plugins = list(subject.project.plugins.plugins())

        assert plugins

        errors = []

        def _collect_error(manager: PluginLocationRemoveManager) -> None:
            errors.append(manager.message)

        with mock.patch(
            "meltano.core.plugin_location_remove.PluginSettingsService.reset",
        ) as reset:
            reset.side_effect = OperationalError(
                "DELETE FROM plugin_settings WHERE plugin_settings.namespace = ?",
                ("extractors.tap-csv.default"),
                "attempt to write a readonly database",
            )
            removed_plugins, _ = subject.remove_plugins(
                plugins,
                removal_manager_status_cb=_collect_error,
            )

        assert removed_plugins == 0
        assert errors.count("attempt to write a readonly database") == len(plugins)

    @pytest.mark.usefixtures("add", "install", "lock")
    def test_remove_meltano_yml_error(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()

        def raise_permissionerror(filename) -> t.NoReturn:
            raise OSError(errno.EACCES, os.strerror(errno.ENOENT), filename)

        plugins = list(subject.project.plugins.plugins())
        with mock.patch.object(
            subject.project.plugins,
            "remove_from_file",
            side_effect=raise_permissionerror,
        ):
            removed_plugins, _total_plugins = subject.remove_plugins(plugins)

        assert removed_plugins == 0

    @pytest.mark.usefixtures("add", "install", "lock")
    def test_remove_installation_error(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()

        def raise_permissionerror(filename) -> t.NoReturn:
            raise OSError(errno.EACCES, os.strerror(errno.ENOENT), filename)

        plugins = list(subject.project.plugins.plugins())

        with mock.patch("meltano.core.plugin_location_remove.shutil.rmtree") as rmtree:
            rmtree.side_effect = raise_permissionerror
            removed_plugins, _total_plugins = subject.remove_plugins(plugins)

        assert removed_plugins == 0

    @pytest.mark.usefixtures("add", "install")
    def test_remove_lockfile_not_found(self, subject: PluginRemoveService) -> None:
        subject.project.refresh()
        plugins = list(subject.project.plugins.plugins(ensure_parent=False))
        removed_plugins, _ = subject.remove_plugins(plugins)

        assert removed_plugins == 0


def _write_yml(project, payload: dict) -> None:
    with project.meltanofile.open("w") as meltano_yml:
        meltano_yml.write(yaml.dump(payload))
    project.refresh()


def _make_lock(project, plugin_type: str, name: str, *, variant: str = "meltanolabs"):
    path = project.dirs.plugin_lock_path(plugin_type, name, variant_name=variant)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    return path


def _make_install(project, plugin_type: str, name: str, *, venv: bool = False):
    path = project.dirs.meltano(plugin_type, name, make_dirs=False)
    path.mkdir(parents=True, exist_ok=True)
    (path / "config.json").write_text("{}\n")
    if venv:
        (path / "venv").mkdir(parents=True, exist_ok=True)
        (path / "venv" / "pyvenv.cfg").write_text("x")
    return path


@pytest.fixture
def configured(subject: PluginRemoveService) -> Generator[None]:
    """Provide a yml writer that always restores the file afterwards."""
    with subject.project.meltanofile.open() as meltano_yml:
        original = meltano_yml.read()

    yield

    with subject.project.meltanofile.open("w") as meltano_yml:
        meltano_yml.write(original)
    subject.project.refresh()
    shutil.rmtree(
        subject.project.dirs.run("plugin-remove", make_dirs=False),
        ignore_errors=True,
    )


class TestPluginRemoveReferences:
    """Reference planning must block unsafe removals before any mutation."""

    @pytest.fixture
    def subject(self, project):
        return PluginRemoveService(project)

    @pytest.fixture
    def add(self, subject: PluginRemoveService) -> Generator[None]:
        with subject.project.meltanofile.open("r") as meltano_yml:
            original = yaml.safe_load(meltano_yml)

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(
                yaml.dump(
                    {
                        "plugins": {
                            "extractors": [TAP_GITLAB],
                            "loaders": [TARGET_CSV],
                        },
                    },
                ),
            )
        subject.project.refresh()

        yield

        with subject.project.meltanofile.open("w") as meltano_yml:
            meltano_yml.write(yaml.dump(original))
        subject.project.refresh()

    def _refs_for(self, err: PluginRemoveBlockedError, name: str):
        (refs,) = [
            references
            for descriptor, references in err.blockers.items()
            if f"'{name}'" in descriptor
        ]
        return refs

    @pytest.mark.usefixtures("add", "configured")
    def test_blocked_by_job(self, subject: PluginRemoveService) -> None:
        _write_yml(
            subject.project,
            {
                "plugins": {
                    "extractors": [TAP_GITLAB],
                    "loaders": [TARGET_CSV],
                },
                "jobs": [
                    {"name": "daily-sync", "tasks": ["tap-gitlab target-csv"]},
                ],
            },
        )
        plugin = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")

        with pytest.raises(PluginRemoveBlockedError) as exc_info:
            subject.remove_plugins([plugin])

        refs = self._refs_for(exc_info.value, "tap-gitlab")
        assert any(ref.kind == "job" and ref.name == "daily-sync" for ref in refs)

        # Nothing was changed: definition, locks and installation stay.
        meltano_yml = yaml.safe_load(subject.project.meltanofile.read_text())
        assert meltano_yml["plugins"]["extractors"][0]["name"] == "tap-gitlab"
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()

    @pytest.mark.usefixtures("add", "configured")
    def test_blocked_by_job_command_token(self, subject: PluginRemoveService) -> None:
        _write_yml(
            subject.project,
            {
                "plugins": {"extractors": [TAP_GITLAB], "loaders": [TARGET_CSV]},
                "jobs": [
                    {
                        "name": "discover",
                        "tasks": ["tap-gitlab:discover target-csv"],
                    },
                ],
            },
        )
        with pytest.raises(PluginRemoveBlockedError) as exc_info:
            subject.remove_plugins(
                [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            )
        refs = self._refs_for(exc_info.value, "tap-gitlab")
        assert any(ref.kind == "job" for ref in refs)

    @pytest.mark.usefixtures("add", "configured")
    def test_blocked_by_elt_schedule(self, subject: PluginRemoveService) -> None:
        _write_yml(
            subject.project,
            {
                "plugins": {"extractors": [TAP_GITLAB], "loaders": [TARGET_CSV]},
                "schedules": [
                    {
                        "name": "gitlab-to-csv",
                        "interval": "@daily",
                        "extractor": "tap-gitlab",
                        "loader": "target-csv",
                        "transform": "skip",
                    },
                ],
            },
        )
        with pytest.raises(PluginRemoveBlockedError) as exc_info:
            subject.remove_plugins(
                [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            )
        refs = self._refs_for(exc_info.value, "tap-gitlab")
        assert any(
            ref.kind == "schedule" and ref.name == "gitlab-to-csv" for ref in refs
        )

    @pytest.mark.usefixtures("configured")
    def test_blocked_by_environment(self, subject: PluginRemoveService) -> None:
        _write_yml(
            subject.project,
            {
                "plugins": {"extractors": [TAP_GITLAB]},
                "environments": [
                    {
                        "name": "prod",
                        "config": {
                            "plugins": {
                                "extractors": [
                                    {"name": "tap-gitlab", "config": {"projects": "x"}},
                                ],
                            },
                        },
                    },
                ],
            },
        )
        with pytest.raises(PluginRemoveBlockedError) as exc_info:
            subject.remove_plugins(
                [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            )
        refs = self._refs_for(exc_info.value, "tap-gitlab")
        assert any(ref.kind == "environment" and ref.name == "prod" for ref in refs)

    @pytest.mark.usefixtures("configured")
    def test_blocked_by_inheriting_plugin(self, subject: PluginRemoveService) -> None:
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        _write_yml(
            subject.project,
            {
                "plugins": {
                    "extractors": [
                        TAP_GITLAB,
                        {
                            "name": "tap-gitlab-secondary",
                            "inherit_from": "tap-gitlab",
                        },
                    ],
                },
            },
        )

        with pytest.raises(PluginRemoveBlockedError) as exc_info:
            subject.remove_plugins(
                [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            )
        refs = self._refs_for(exc_info.value, "tap-gitlab")
        assert any(
            ref.kind == "plugin" and ref.name == "tap-gitlab-secondary" for ref in refs
        )

        # Shared artifacts must survive a blocked removal untouched.
        assert install.exists()
        assert (install / "venv" / "pyvenv.cfg").exists()
        assert lock.exists()

    @pytest.mark.usefixtures("configured")
    def test_remove_plugin_family_in_one_batch(
        self,
        subject: PluginRemoveService,
    ) -> None:
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        parent_lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        child_lock = _make_lock(
            subject.project,
            "extractors",
            "tap-gitlab-secondary",
        )
        _write_yml(
            subject.project,
            {
                "plugins": {
                    "extractors": [
                        TAP_GITLAB,
                        {"name": "tap-gitlab-secondary", "inherit_from": "tap-gitlab"},
                    ],
                },
            },
        )

        removed, total = subject.remove_plugins(
            [
                ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab"),
                ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab-secondary"),
            ],
        )

        assert (removed, total) == (2, 2)
        assert not install.exists()
        assert not parent_lock.exists()
        assert not child_lock.exists()
        meltano_yml = yaml.safe_load(subject.project.meltanofile.read_text())
        assert meltano_yml.get("plugins", {}).get("extractors", []) == []
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()

    @pytest.mark.usefixtures("configured")
    def test_lock_glob_does_not_match_name_prefix(
        self,
        subject: PluginRemoveService,
    ) -> None:
        tap_lock = _make_lock(
            subject.project,
            "extractors",
            "tap",
            variant="v1",
        )
        other_lock = _make_lock(
            subject.project,
            "extractors",
            "tap-gitlab",
            variant="v2",
        )
        _write_yml(
            subject.project,
            {
                "plugins": {
                    "extractors": [
                        {"name": "tap", "pip_url": "tap-pkg"},
                        {"name": "tap-gitlab", "pip_url": "gitlab-pkg"},
                    ],
                },
            },
        )

        subject.remove_plugins([ProjectPlugin(PluginType.EXTRACTORS, "tap")])

        assert not tap_lock.exists()
        assert other_lock.exists()


class TestPluginRemovePlanning:
    """Dry-run, stale plan and rollback behavior."""

    @pytest.fixture
    def subject(self, project):
        return PluginRemoveService(project)

    @pytest.fixture
    def add(self, subject: PluginRemoveService) -> Generator[None]:
        with subject.project.meltanofile.open("r") as meltano_yml:
            original = meltano_yml.read()
        subject.project.meltanofile.write_text(
            yaml.dump({"plugins": {"extractors": [TAP_GITLAB]}}),
        )
        subject.project.refresh()
        yield
        subject.project.meltanofile.write_text(original)
        subject.project.refresh()

    @pytest.mark.usefixtures("add")
    def test_dry_run_reports_without_changes(
        self,
        subject: PluginRemoveService,
    ) -> None:
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        subject.project.refresh()

        statuses: list[PluginLocationRemoveStatus] = []
        removed, total = subject.remove_plugins(
            [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            removal_manager_status_cb=lambda manager: statuses.append(
                manager.remove_status,
            ),
            dry_run=True,
        )

        assert (removed, total) == (1, 1)
        assert PluginLocationRemoveStatus.PENDING in statuses
        # Definition, lock and installation are untouched.
        meltano_yml = yaml.safe_load(subject.project.meltanofile.read_text())
        assert meltano_yml["plugins"]["extractors"][0]["name"] == "tap-gitlab"
        assert install.exists()
        assert (install / "venv").exists()
        assert lock.exists()

    @pytest.mark.usefixtures("add", "configured")
    def test_dry_run_reports_blockers_without_raising(
        self,
        subject: PluginRemoveService,
    ) -> None:
        _write_yml(
            subject.project,
            {
                "plugins": {"extractors": [TAP_GITLAB]},
                "jobs": [{"name": "j", "tasks": ["tap-gitlab"]}],
            },
        )
        blockers: list = []
        removed, total = subject.remove_plugins(
            [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            blockers_cb=lambda _plugin, refs: blockers.extend(refs),
            dry_run=True,
        )
        assert (removed, total) == (0, 1)
        assert any(ref.kind == "job" for ref in blockers)
        meltano_yml = yaml.safe_load(subject.project.meltanofile.read_text())
        assert meltano_yml["plugins"]["extractors"][0]["name"] == "tap-gitlab"

    @pytest.mark.usefixtures("add")
    def test_db_error_rolls_back_definition_and_locks(
        self,
        subject: PluginRemoveService,
    ) -> None:
        lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        _make_install(subject.project, "extractors", "tap-gitlab")
        subject.project.refresh()
        before = subject.project.meltanofile.read_bytes()

        with mock.patch(
            "meltano.core.plugin_location_remove.PluginSettingsService.reset",
        ) as reset:
            reset.side_effect = OperationalError(
                "DELETE FROM plugin_settings",
                {},
                "attempt to write a readonly database",
            )
            removed, total = subject.remove_plugins(
                [ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")],
            )

        assert (removed, total) == (0, 1)
        assert subject.project.meltanofile.read_bytes() == before
        assert lock.exists()
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()

    @pytest.mark.usefixtures("add", "configured")
    def test_stale_plan_with_new_blocker_is_not_committed(
        self,
        subject: PluginRemoveService,
    ) -> None:
        _write_yml(
            subject.project,
            {"plugins": {"extractors": [TAP_GITLAB]}},
        )
        requested = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")
        plan = subject._plan(  # noqa: SLF001
            requested,
            batch={(PluginType.EXTRACTORS, "tap-gitlab")},
        )
        assert plan["references"] == []

        # The project changes after planning: an inheriting plugin is added.
        _write_yml(
            subject.project,
            {
                "plugins": {
                    "extractors": [
                        TAP_GITLAB,
                        {"name": "tap-gitlab-secondary", "inherit_from": "tap-gitlab"},
                    ],
                },
            },
        )

        with pytest.raises(StaleRemovalPlanError):
            subject._commit(plan)  # noqa: SLF001

        # Nothing was removed from the changed project.
        meltano_yml = yaml.safe_load(subject.project.meltanofile.read_text())
        assert [
            p["name"] for p in meltano_yml["plugins"]["extractors"]
        ] == ["tap-gitlab", "tap-gitlab-secondary"]
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()

    @pytest.mark.usefixtures("add", "configured")
    def test_stale_plan_after_external_removal_is_not_committed(
        self,
        subject: PluginRemoveService,
    ) -> None:
        _write_yml(
            subject.project,
            {"plugins": {"extractors": [TAP_GITLAB]}},
        )
        requested = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")
        plan = subject._plan(  # noqa: SLF001
            requested,
            batch={(PluginType.EXTRACTORS, "tap-gitlab")},
        )

        # The plugin disappears from the project after planning.
        _write_yml(subject.project, {"plugins": {"extractors": []}})

        with pytest.raises(StaleRemovalPlanError):
            subject._commit(plan)  # noqa: SLF001
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()


class TestPluginRemoveRecovery:
    """Crash/partial-failure convergence via the removal journal."""

    @pytest.fixture
    def subject(self, project):
        return PluginRemoveService(project)

    def _journal(
        self,
        subject: PluginRemoveService,
        plugin: ProjectPlugin,
        *,
        phase: str,
        steps: list[str] | None = None,
        pending: list[str] | None = None,
    ) -> PluginRemoveJournal:
        journal = PluginRemoveJournal(subject.project, plugin)
        journal.dir.mkdir(parents=True, exist_ok=True)
        journal.phase = phase
        journal.steps_completed = steps or []
        journal.cleanup_pending = pending or []
        journal.fingerprint = "x"
        journal.save()
        return journal

    @pytest.mark.usefixtures("configured")
    def test_resume_committed_journal_finishes_cleanup(
        self,
        subject: PluginRemoveService,
    ) -> None:
        plugin = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")
        _write_yml(subject.project, {"plugins": {"extractors": []}})
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        manager = InstallationRemoveManager(plugin, subject.project)
        journal = self._journal(
            subject,
            plugin,
            phase=PluginRemoveJournal.COMMITTED,
            steps=["meltano_yml", "locks", "cache", "db"],
            pending=[str(path) for path in manager.cleanup_paths],
        )
        assert journal.path.exists()

        removed, total = subject.remove_plugins([plugin])

        assert (removed, total) == (1, 1)
        assert not install.exists()
        assert not journal.path.exists()

    @pytest.mark.usefixtures("configured")
    def test_stale_committed_journal_is_discarded_when_readded(
        self,
        subject: PluginRemoveService,
    ) -> None:
        plugin = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")
        _write_yml(subject.project, {"plugins": {"extractors": [TAP_GITLAB]}})
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        manager = InstallationRemoveManager(plugin, subject.project)
        self._journal(
            subject,
            plugin,
            phase=PluginRemoveJournal.COMMITTED,
            steps=["meltano_yml", "locks", "cache", "db"],
            pending=[str(path) for path in manager.cleanup_paths],
        )
        subject.project.refresh()

        removed, total = subject.remove_plugins([plugin])

        assert (removed, total) == (1, 1)
        assert not install.exists()
        assert not lock.exists()
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()

    @pytest.mark.usefixtures("configured")
    def test_planned_journal_with_definition_intact_rolls_back(
        self,
        subject: PluginRemoveService,
    ) -> None:
        plugin = ProjectPlugin(PluginType.EXTRACTORS, "tap-gitlab")
        _write_yml(subject.project, {"plugins": {"extractors": [TAP_GITLAB]}})
        install = _make_install(
            subject.project,
            "extractors",
            "tap-gitlab",
            venv=True,
        )
        lock = _make_lock(subject.project, "extractors", "tap-gitlab")
        # Simulate a crash after the lock was moved into the backup.
        moved_lock = lock
        moved_lock.unlink()
        journal = self._journal(
            subject,
            plugin,
            phase=PluginRemoveJournal.PLANNED,
            steps=["locks"],
        )
        backup_lock = journal.backup_dir / "locks" / "tap-gitlab--meltanolabs.lock"
        backup_lock.parent.mkdir(parents=True)
        backup_lock.write_text("{}\n")
        journal.record_move("locks", "tap-gitlab--meltanolabs.lock")

        removed, total = subject.remove_plugins([plugin])

        assert (removed, total) == (1, 1)
        # Rolled back, then removed fresh in the same operation.
        assert not lock.exists()
        assert not install.exists()
        assert not journal.path.exists()

    def test_undefined_plugin_is_idempotent_and_not_journaled(
        self,
        subject: PluginRemoveService,
    ) -> None:
        removed, total = subject.remove_plugins(
            [ProjectPlugin(PluginType.EXTRACTORS, "does-not-exist")],
        )
        assert (removed, total) == (0, 1)
        assert not subject.project.dirs.run("plugin-remove", make_dirs=False).exists()
