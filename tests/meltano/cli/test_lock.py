"""Test the lock CLI command."""

from __future__ import annotations

import shutil
import typing as t

import pytest

from meltano.cli import cli
from meltano.cli.utils import CliError
from meltano.core.error import ProjectReadonly
from meltano.core.lock_snapshot_service import SnapshotExistsError
from meltano.core.plugin.base import PluginType
from meltano.core.plugin.project_plugin import ProjectPlugin
from meltano.core.plugin_lock_service import PluginLockService

if t.TYPE_CHECKING:
    from click.testing import CliRunner

    from meltano.core.project import Project


class TestLock:
    @pytest.mark.order(0)
    @pytest.mark.usefixtures("project")
    def test_lock_no_plugins(self, cli_runner: CliRunner) -> None:
        exception_message = "No matching plugin(s) found"

        result = cli_runner.invoke(cli, ["lock"])
        assert exception_message == str(result.exception)

        result = cli_runner.invoke(cli, ["lock", "--update"])
        assert exception_message == str(result.exception)

    @pytest.mark.order(1)
    @pytest.mark.usefixtures("tap", "target")
    def test_lockfile_exists(
        self,
        cli_runner: CliRunner,
        project: Project,
    ) -> None:
        lockfiles = list(project.dirs.root_plugins().glob("./*/*.lock"))
        assert len(lockfiles) == 2

        result = cli_runner.invoke(
            cli,
            [
                "--log-level=debug",
                "--log-format=uncolored",
                "lock",
            ],
        )
        assert result.exit_code == 0
        assert "Lockfile exists for extractor tap-mock" in result.stderr
        assert "Lockfile exists for loader target-mock" in result.stderr
        assert "Locked definition" not in result.stderr

    @pytest.mark.order(2)
    def test_lockfile_update(
        self,
        cli_runner: CliRunner,
        project: Project,
        tap: ProjectPlugin,
        hub_endpoints: dict[str, dict],
    ) -> None:
        subject = PluginLockService(project)
        tap_lock_path = subject.plugin_lock_path(plugin=tap, variant_name=tap.variant)

        assert tap_lock_path.exists()
        old_definition = subject.load_definition(
            plugin_type=tap.type,
            plugin_name=tap.name,
            variant_name=tap.variant,
        )
        old_variant = old_definition.find_variant(tap.variant)

        # Update the plugin in Hub
        hub_endpoints["/extractors/tap-mock--meltano"]["settings"].append(
            {
                "name": "foo",
                "value": "bar",
            },
        )

        result = cli_runner.invoke(
            cli,
            [
                "--log-level=debug",
                "--log-format=uncolored",
                "lock",
                "--update",
            ],
        )
        assert result.exit_code == 0
        assert result.stderr.count("Lockfile exists") == 0
        assert result.stderr.count("Locked definition") == 2
        new_definition = subject.load_definition(
            plugin_type=tap.type,
            plugin_name=tap.name,
            variant_name=tap.variant,
        )
        new_variant = new_definition.find_variant(tap.variant)
        assert len(new_variant.settings) == len(old_variant.settings) + 1

        new_setting = new_variant.settings[-1]
        assert new_setting.name == "foo"
        assert new_setting.value == "bar"

    @pytest.mark.order(3)
    @pytest.mark.usefixtures("tap", "inherited_tap", "hub_endpoints")
    def test_lockfile_update_extractors(
        self,
        cli_runner: CliRunner,
        project: Project,
    ) -> None:
        lockfiles = list(project.dirs.root_plugins().glob("./*/*.lock"))
        # 1 tap, 1 target
        assert len(lockfiles) == 2

        result = cli_runner.invoke(
            cli,
            [
                "--log-level=debug",
                "--log-format=uncolored",
                "lock",
                "--update",
                "--plugin-type",
                "extractor",
            ],
        )
        assert result.exit_code == 0
        assert "Lockfile exists" not in result.stderr
        assert "Locked definition for extractor tap-mock" in result.stderr
        assert "Extractor tap-mock-inherited is an inherited plugin" in result.stderr

    @pytest.mark.usefixtures("project")
    def test_lock_plugin_not_found(self, cli_runner: CliRunner) -> None:
        result = cli_runner.invoke(cli, ["lock", "not-a-plugin"])
        assert result.exit_code == 1
        assert isinstance(result.exception, CliError)
        assert "No matching plugin(s) found" in str(result.exception)


@pytest.mark.usefixtures("tap", "target", "inherited_tap")
class TestBatchLock:
    @pytest.fixture(autouse=True)
    def _clear_batch_locks(self, project: Project):
        """Keep batch tests independent under randomized test ordering."""
        pointer = project.dirs.plugin_lock_snapshot_pointer()
        if pointer.exists():
            pointer.unlink()

        snapshots = project.dirs.plugin_lock_snapshots_dir()
        if snapshots.exists():
            shutil.rmtree(snapshots)

        staging = project.dirs.plugin_lock_staging_dir()
        if staging.exists():
            shutil.rmtree(staging)

    @staticmethod
    def _invoke(cli_runner: CliRunner, *args: str):
        return cli_runner.invoke(
            cli,
            ["--log-level=debug", "--log-format=uncolored", "lock", *args],
        )

    def test_batch_publishes_snapshot(
        self,
        cli_runner: CliRunner,
        project: Project,
    ) -> None:
        result = self._invoke(cli_runner, "--batch")
        assert result.exit_code == 0
        assert "Locked definition for extractor tap-mock" in result.stderr
        assert "Locked definition for loader target-mock" in result.stderr
        assert "Published lock snapshot" in result.stderr
        assert "Extractor tap-mock-inherited is an inherited plugin" in result.stderr
        assert project.dirs.plugin_lock_snapshot_pointer().exists()

    def test_batch_update_relocks_after_success(
        self,
        cli_runner: CliRunner,
    ) -> None:
        first = self._invoke(cli_runner, "--batch")
        assert first.exit_code == 0

        # After a successful publication, targets are re-resolved for update;
        # reuse is reserved for retries following a failure.
        second = self._invoke(cli_runner, "--batch", "--update")
        assert second.exit_code == 0
        assert "Locked definition for extractor tap-mock" in second.stderr
        assert "Locked definition for loader target-mock" in second.stderr
        assert "Reused cached definition" not in second.stderr

    def test_batch_without_update_when_snapshot_exists(
        self,
        cli_runner: CliRunner,
    ) -> None:
        first = self._invoke(cli_runner, "--batch")
        assert first.exit_code == 0

        second = self._invoke(cli_runner, "--batch")
        assert second.exit_code == 1
        assert isinstance(second.exception, SnapshotExistsError)
        assert "already published" in str(second.exception)

    def test_batch_failure_keeps_previous_snapshot(
        self,
        cli_runner: CliRunner,
        project: Project,
    ) -> None:
        first = self._invoke(cli_runner, "--batch")
        assert first.exit_code == 0
        first_manifest = PluginLockService(project)
        pointer_before = project.dirs.plugin_lock_snapshot_pointer().read_text()

        # Add a plugin whose Hub resolution fails.
        bad_plugin = ProjectPlugin(
            PluginType.EXTRACTORS,
            "this-returns-500",
            variant="original",
        )
        with project.plugins.update_plugins() as plugins:
            if PluginType.EXTRACTORS not in plugins:
                plugins[PluginType.EXTRACTORS] = []
            plugins[PluginType.EXTRACTORS].append(bad_plugin)

        second = self._invoke(cli_runner, "--batch", "--update")
        assert second.exit_code == 1
        assert isinstance(second.exception, CliError)
        assert "Failed to lock 1 plugin(s)" in str(second.exception)
        assert "Failed to lock extractor this-returns-500" in second.stderr

        # The pointer is untouched and still reads the first snapshot.
        assert (
            project.dirs.plugin_lock_snapshot_pointer().read_text()
            == pointer_before
        )
        assert first_manifest is not None

        # Remove the failing plugin so later tests see the original project.
        with project.plugins.update_plugins() as plugins:
            plugins[PluginType.EXTRACTORS].remove(bad_plugin)

    def test_batch_readonly(
        self,
        cli_runner: CliRunner,
        project: Project,
    ) -> None:
        project.readonly = True
        try:
            result = self._invoke(cli_runner, "--batch")
        finally:
            project.readonly = False

        assert result.exit_code == 1
        assert isinstance(result.exception, ProjectReadonly)
        assert not project.dirs.plugin_lock_snapshot_pointer().exists()
