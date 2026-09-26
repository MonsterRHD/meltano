from __future__ import annotations

import asyncio
import json
import os
import stat
import typing as t
from unittest.mock import AsyncMock, patch

import pytest
import yaml

from meltano.core.install_transaction.errors import (
    ExecutabilityCheckError,
    StagingBuildError,
    StalePlanError,
)
from meltano.core.install_transaction.paths import InstallPaths
from meltano.core.install_transaction.plan import (
    InstallPlan,
    InstallPlanService,
)
from meltano.core.install_transaction.staging import StagingBuilder
from meltano.core.install_transaction.transaction import (
    InstallTransaction,
)
from meltano.core.venv_service import VirtualEnv

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

EXECUTABLE_NAME = "fake-exec"


def write_plugin(
    project: Project,
    name: str,
    plugin_type: str = "extractors",
    pip_url: str = "alpha-pkg",
) -> ProjectPlugin:
    payload = {
        "plugins": {
            plugin_type: [
                {
                    "name": name,
                    "namespace": name.replace("-", "_"),
                    "pip_url": pip_url,
                    "executable": EXECUTABLE_NAME,
                },
            ],
        },
    }
    with project.meltanofile.open("w") as file:
        file.write(yaml.dump(payload))
    project.refresh()
    return next(iter(project.plugins.get_plugins_of_type(_type(plugin_type))))


def _type(value: str) -> t.Any:
    from meltano.core.plugin import PluginType

    return PluginType(value)


def reason() -> t.Any:
    from meltano.core.plugin_install_service import PluginInstallReason

    return PluginInstallReason.INSTALL


def seed_reusable(
    project: Project,
    plugin: ProjectPlugin,
    plan: InstallPlan,
) -> InstallPaths:
    """Create a complete, checked staging environment for the plan."""
    paths = InstallPaths(project, plugin)
    staging = paths.staging_dir(plan.plan_id)
    staging.mkdir(parents=True, exist_ok=True)
    paths.plan_file(plan.plan_id).write_text(json.dumps(plan.to_dict()))

    venv_path = paths.staging_venv(plan.plan_id)
    bin_dir = venv_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    python = bin_dir / "python"
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(python.stat().st_mode | stat.S_IXUSR)

    VirtualEnv(venv_path).write_fingerprint(plan.pip_install_args)

    checked = paths.marker(plan.plan_id, "checked")
    checked.parent.mkdir(parents=True, exist_ok=True)
    checked.touch()
    return paths


class TestInstallTransaction:
    def test_fast_path_after_commit(
        self,
        project_function: Project,
    ) -> None:
        plugin = write_plugin(project_function, "tap-custom")
        service = InstallPlanService(project_function)
        plan = service.resolve(plugin, reason())
        seed_reusable(project_function, plugin, plan)

        transaction = InstallTransaction(project_function, plan_service=service)
        # First run commits the reusable staging
        result_1 = asyncio.run(transaction.execute(plugin, reason()))
        assert result_1.status == "success"

        # Second run: fast path, builder never invoked
        mock_build = AsyncMock(side_effect=AssertionError("no build expected"))
        transaction.builder.build = mock_build
        result_2 = asyncio.run(transaction.execute(plugin, reason()))
        assert result_2.status == "skipped"
        assert result_2.message == "Requirements have not changed"
        mock_build.assert_not_called()

    def test_dry_run_no_writes(
        self,
        project_function: Project,
    ) -> None:
        from meltano.core.plugin_install_service import (
            PluginInstallService,
            PluginInstallStatus,
        )

        def snapshot() -> set[str]:
            return {
                str(path.relative_to(project_function.root))
                for path in project_function.root.rglob("*")
                if path.is_file()
            }

        before = snapshot()
        service = PluginInstallService(project_function, dry_run=True)
        state = asyncio.run(
            service.install_plugin_async(
                write_plugin(project_function, "tap-custom"),
            ),
        )
        after = snapshot()

        assert state.status == PluginInstallStatus.RUNNING
        assert "Would install" in state.message
        assert before == after

    @pytest.mark.parametrize("failure_kind", ["staging", "check", "stale"])
    def test_failures_preserve_previous(
        self,
        project_function: Project,
        failure_kind: str,
    ) -> None:
        from dataclasses import replace

        plugin = write_plugin(project_function, "tap-custom", pip_url="alpha-pkg")
        plan_service = InstallPlanService(project_function)
        plan_a = plan_service.resolve(plugin, reason())
        seed_reusable(project_function, plugin, plan_a)

        transaction = InstallTransaction(project_function, plan_service=plan_service)
        result_a = asyncio.run(transaction.execute(plugin, reason()))
        assert result_a.status == "success"

        # The next attempt targets a different plan, so the fast path is skipped
        plugin = write_plugin(project_function, "tap-custom", pip_url="beta-pkg")
        plan_b = plan_service.resolve(plugin, reason())
        paths = seed_reusable(project_function, plugin, plan_b)

        if failure_kind == "staging":
            paths.marker(plan_b.plan_id, "checked").unlink()
            transaction.builder.build = AsyncMock(
                side_effect=StagingBuildError(reason="dep boom", instruction="retry"),
            )
            expected_plan = plan_b.plan_id
        elif failure_kind == "check":
            paths.marker(plan_b.plan_id, "checked").unlink()
            transaction.builder.build = AsyncMock()
            transaction.checker.check = AsyncMock(
                side_effect=ExecutabilityCheckError(
                    reason="probe boom",
                    instruction="retry",
                ),
            )
            expected_plan = plan_b.plan_id
        else:
            # Make the committer's re-resolution return a different plan
            call_count = 0
            real_resolve = plan_service.resolve

            def patched_resolve(plugin, reason, *, offline=False):
                nonlocal call_count
                result = real_resolve(plugin, reason, offline=offline)
                call_count += 1
                if call_count >= 2:
                    return replace(result, plan_id="totally-different")
                return result

            plan_service.resolve = patched_resolve
            expected_plan = plan_b.plan_id

        outcome = asyncio.run(transaction.execute(plugin, reason()))
        assert outcome.status == "error"

        # Previous version intact and runnable
        assert paths.venv.joinpath("bin/python").exists()
        on_disk = json.loads(paths.state_path.read_text())
        assert on_disk["plan_id"] == plan_a.plan_id

        # A recovery record exists for the rejected attempt
        records = list(paths.recovery_root.glob("*.json"))
        assert records
        open_record = next(
            json.loads(path.read_text())
            for path in records
            if json.loads(path.read_text())["status"] == "open"
        )
        assert open_record["plan_id"] == expected_plan

    def test_first_install_failure_then_retry(
        self,
        project_function: Project,
    ) -> None:
        plugin = write_plugin(project_function, "tap-custom")
        plan_service = InstallPlanService(project_function)
        transaction = InstallTransaction(project_function, plan_service=plan_service)

        transaction.builder.build = AsyncMock(
            side_effect=StagingBuildError(reason="boom", instruction="retry"),
        )
        result = asyncio.run(transaction.execute(plugin, reason()))
        assert result.status == "error"

        paths = InstallPaths(project_function, plugin)
        assert not paths.venv.exists()
        assert not paths.state_path.exists()
        assert list(paths.recovery_root.glob("*.json"))

        # Retry with complete staged environment
        plan = plan_service.resolve(plugin, reason())
        seed_reusable(project_function, plugin, plan)
        retry = asyncio.run(transaction.execute(plugin, reason()))
        assert retry.status == "success"

    def test_concurrent_different_plugins_parallel(
        self,
        project_function: Project,
    ) -> None:
        tap = write_plugin(project_function, "tap-custom", "extractors")
        target = write_plugin(project_function, "target-custom", "loaders")

        plan_service = InstallPlanService(project_function)
        tap_plan = plan_service.resolve(tap, reason())
        target_plan = plan_service.resolve(target, reason())

        tap_paths = seed_reusable(project_function, tap, tap_plan)
        target_paths = seed_reusable(project_function, target, target_plan)

        transaction = InstallTransaction(project_function, plan_service=plan_service)

        async def both() -> list[str]:
            results = await asyncio.gather(
                transaction.execute(tap, reason()),
                transaction.execute(target, reason()),
            )
            return [r.status for r in results]

        statuses = asyncio.run(both())
        assert statuses == ["success", "success"]

        # Independent locks and state
        assert tap_paths.commit_lock_path != target_paths.commit_lock_path
        assert tap_paths.state_path.exists()
        assert target_paths.state_path.exists()

    def test_same_plugin_single_committer(
        self,
        project_function: Project,
    ) -> None:
        plugin = write_plugin(project_function, "tap-custom")
        plan_service = InstallPlanService(project_function)
        plan = plan_service.resolve(plugin, reason())
        paths = seed_reusable(project_function, plugin, plan)

        async def run_pair() -> list[str]:
            transaction = InstallTransaction(project_function, plan_service=plan_service)
            results = await asyncio.gather(
                transaction.execute(plugin, reason()),
                transaction.execute(plugin, reason()),
            )
            return [r.status for r in results]

        statuses = asyncio.run(run_pair())
        assert sorted(statuses) == ["skipped", "success"]
        assert len(json.loads(paths.state_path.read_text())["plan_id"]) == 64
