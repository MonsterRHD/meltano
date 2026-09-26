from __future__ import annotations

import typing as t
from unittest import mock

import pytest

from asserts import assert_cli_runner
from meltano.cli import cli
from meltano.core.plugin import PluginType
from meltano.core.plugin_remove_service import PluginReference, PluginRemoveBlockedError
from meltano.core.project_add_service import PluginAlreadyAddedException

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project_add_service import ProjectAddService


class TestCliRemove:
    @pytest.fixture(scope="class")
    @classmethod
    def tap_gitlab(cls, project_add_service: ProjectAddService):
        try:
            return project_add_service.add(PluginType.EXTRACTORS, "tap-gitlab")
        except PluginAlreadyAddedException as err:
            return err.plugin

    def test_remove(self, project, tap: ProjectPlugin, cli_runner) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(
                cli,
                ["remove", f"--plugin-type={tap.type.value}", tap.name],
            )

            assert_cli_runner(result)

            remove_plugins_mock.assert_called_once_with(project, [tap])

    def test_remove_multiple(self, project, tap, tap_gitlab, cli_runner) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(
                cli,
                ["remove", "--plugin-type=extractors", tap.name, tap_gitlab.name],
            )
            assert_cli_runner(result)

            remove_plugins_mock.assert_called_once_with(project, [tap, tap_gitlab])

    def test_remove_type_name(self, project, tap, target, cli_runner) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(
                cli,
                ["remove", "--plugin-type=extractor", tap.name],
            )

            assert_cli_runner(result)

            remove_plugins_mock.assert_called_with(project, [tap])

            result = cli_runner.invoke(
                cli,
                ["remove", "--plugin-type=loader", target.name],
            )
            assert_cli_runner(result)

            remove_plugins_mock.assert_called_with(project, [target])

            assert remove_plugins_mock.call_count == 2

    def test_remove_no_plugin_type(self, project, tap, tap_gitlab, cli_runner) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(cli, ["remove", tap.name, tap_gitlab.name])
            assert_cli_runner(result)

            remove_plugins_mock.assert_called_once_with(project, [tap, tap_gitlab])

    def test_remove_with_explicit_plugin_type(
        self,
        project,
        tap,
        tap_gitlab,
        cli_runner,
    ) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(
                cli,
                [
                    "remove",
                    "--plugin-type",
                    "extractors",
                    tap.name,
                    tap_gitlab.name,
                ],
            )
            assert_cli_runner(result)

            remove_plugins_mock.assert_called_once_with(project, [tap, tap_gitlab])

    def test_remove_dry_run(
        self,
        project,
        tap,
        cli_runner,
    ) -> None:
        with mock.patch("meltano.cli.remove.remove_plugins") as remove_plugins_mock:
            result = cli_runner.invoke(
                cli,
                ["remove", "--dry-run", f"--plugin-type={tap.type.value}", tap.name],
            )
            assert_cli_runner(result)

            remove_plugins_mock.assert_called_once_with(
                project,
                [tap],
                dry_run=True,
            )

    def test_remove_blocked_exits_nonzero(
        self,
        tap,
        cli_runner,
    ) -> None:
        blocked = PluginRemoveBlockedError(
            {
                f"{tap.type.descriptor} '{tap.name}'": [
                    PluginReference("job", "daily-sync"),
                ],
            },
        )
        with mock.patch(
            "meltano.cli.remove.PluginRemoveService.remove_plugins",
            side_effect=blocked,
        ):
            result = cli_runner.invoke(
                cli,
                ["remove", f"--plugin-type={tap.type.value}", tap.name],
            )

        # `MeltanoError` is converted to exit code 1 by the CLI entrypoint;
        # under the test runner the original exception is surfaced.
        assert isinstance(result.exception, PluginRemoveBlockedError)
        assert result.exception.exit_code() == 1
        assert "daily-sync" in str(result.exception)
