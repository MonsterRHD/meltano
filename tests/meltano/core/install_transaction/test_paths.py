from __future__ import annotations

import typing as t

import pytest

from meltano.core.install_transaction.paths import (
    BACKUP_PREFIX,
    InstallPaths,
)
from meltano.core.plugin import PluginType
from meltano.core.plugin.project_plugin import ProjectPlugin
from meltano.core.project_plugins_service import PluginAlreadyAddedException

if t.TYPE_CHECKING:
    from meltano.core.project import Project


@pytest.fixture
def tap(project_add_service) -> ProjectPlugin:
    try:
        return project_add_service.add(
            PluginType.EXTRACTORS,
            "tap-mock",
            variant="meltano",
        )
    except PluginAlreadyAddedException as err:  # pragma: no cover
        return err.plugin


@pytest.fixture
def target(project_add_service) -> ProjectPlugin:
    try:
        return project_add_service.add(
            PluginType.LOADERS,
            "target-mock",
        )
    except PluginAlreadyAddedException as err:  # pragma: no cover
        return err.plugin


@pytest.fixture
def paths(project: Project, tap: ProjectPlugin) -> InstallPaths:
    return InstallPaths(project, tap)


class TestInstallPaths:
    def test_venv_matches_legacy_layout(
        self,
        project: Project,
        paths: InstallPaths,
        tap: ProjectPlugin,
    ) -> None:
        assert paths.venv == project.dirs.venvs(tap.type, tap.plugin_dir_name)
        assert paths.root == project.dirs.plugin(tap, make_dirs=False)

    def test_derived_paths(self, paths: InstallPaths) -> None:
        plan_id = "abc123"
        assert paths.staging_dir(plan_id) == paths.staging_root / plan_id
        assert paths.staging_venv(plan_id) == (
            paths.staging_root / plan_id / "venv"
        )
        assert paths.plan_file(plan_id).name == "plan.json"
        assert paths.candidate_state(plan_id).name == "install.json"
        assert paths.marker(plan_id, "deps-installed").name == "deps-installed"
        assert paths.recovery_record_path("rec").name == "rec.json"
        assert paths.commit_lock_path.name == "commit.lock"
        assert paths.venv_backup(plan_id).name == f"{BACKUP_PREFIX}{plan_id}"

    def test_paths_do_not_touch_disk(
        self,
        project: Project,
    ) -> None:
        # An in-memory plugin unique to this test keeps the check independent
        # of ordering, without touching the Hub or the project files.
        pristine = ProjectPlugin(
            PluginType.EXTRACTORS,
            "tap-mock-pristine",
            namespace="tap_mock_pristine",
            pip_url="pristine-pkg",
        )

        untouched = InstallPaths(project, pristine)
        assert not untouched.root.exists()
        for path in (
            untouched.venv,
            untouched.state_path,
            untouched.staging_root,
            untouched.recovery_root,
            untouched.locks_dir,
        ):
            assert not path.exists()

    def test_ensure_layout(self, paths: InstallPaths) -> None:
        paths.ensure_layout()
        assert paths.staging_root.is_dir()
        assert paths.recovery_root.is_dir()
        assert paths.locks_dir.is_dir()

        # Idempotent
        paths.ensure_layout()
        assert paths.staging_root.is_dir()

    def test_distinct_plugins_have_distinct_roots(
        self,
        project: Project,
        paths: InstallPaths,
        target: ProjectPlugin,
    ) -> None:
        other = InstallPaths(project, target)
        assert other.root != paths.root
        assert other.commit_lock_path != paths.commit_lock_path
        assert other.staging_root != paths.staging_root
