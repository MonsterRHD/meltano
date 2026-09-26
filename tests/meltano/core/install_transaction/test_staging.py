from __future__ import annotations

import asyncio
import os
import stat
import typing as t
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from meltano.core.install_transaction.errors import (
    ExecutabilityCheckError,
)
from meltano.core.install_transaction.plan import (
    InstallPlan,
    InstallPlanService,
)
from meltano.core.install_transaction.staging import (
    CHECKED,
    DEPS_INSTALLED,
    ExecutabilityChecker,
    StagingBuilder,
)
from meltano.core.install_transaction.paths import InstallPaths
from meltano.core.venv_service import VirtualEnv

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

EXECUTABLE_NAME = "fake-exec"


def write_custom_plugin(project: Project, pip_url: str) -> ProjectPlugin:
    payload = {
        "plugins": {
            "extractors": [
                {
                    "name": "tap-custom",
                    "namespace": "tap_custom",
                    "pip_url": pip_url,
                    "executable": EXECUTABLE_NAME,
                },
            ],
        },
    }
    with project.meltanofile.open("w") as file:
        file.write(yaml.dump(payload))
    project.refresh()
    return next(project.plugins.plugins())


@pytest.fixture
def resolved(project_function: Project) -> tuple[ProjectPlugin, InstallPlan]:
    plugin = write_custom_plugin(project_function, "alpha-pkg")
    plan = InstallPlanService(project_function).resolve(plugin, _reason())
    return plugin, plan


def reason() -> t.Any:
    from meltano.core.plugin_install_service import PluginInstallReason

    return PluginInstallReason.INSTALL


_reason = reason


class TestStagingBuilder:
    def test_prepare(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        builder = StagingBuilder(project_function)
        paths = builder.prepare(plugin, plan)

        assert paths.staging_dir(plan.plan_id).is_dir()
        plan_data = paths.plan_file(plan.plan_id)
        assert plan_data.exists()
        assert "plan_id" in plan_data.read_text()

    def test_build_targets_staging_and_preserves_final(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)

        # Pre-existing committed venv that must remain untouched
        paths.venv.mkdir(parents=True)
        sentinel = paths.venv / "sentinel.txt"
        sentinel.write_text("old")
        before = sorted(
            str(p.relative_to(paths.venv)) for p in paths.venv.rglob("*")
        )

        paths.staging_venv(plan.plan_id).mkdir(parents=True)
        fake_service = MagicMock()
        fake_service.create = AsyncMock()
        fake_service.pip_install = AsyncMock()
        fake_service.clean_run_files = MagicMock()
        fake_service.venv = VirtualEnv(paths.staging_venv(plan.plan_id))

        captured: dict[str, t.Any] = {}

        def from_plugin(project, plugin, *, venv_path):
            captured["venv_path"] = venv_path
            return fake_service

        builder = StagingBuilder(project_function)
        with patch(
            "meltano.core.install_transaction.staging.VirtualEnvService",
            MagicMock(from_plugin=staticmethod(from_plugin)),
        ):
            asyncio.run(builder.build(plugin, plan))

        assert captured["venv_path"] == paths.staging_venv(plan.plan_id)
        after = sorted(
            str(p.relative_to(paths.venv)) for p in paths.venv.rglob("*")
        )
        assert before == after
        assert sentinel.read_text() == "old"
        assert (
            VirtualEnv(paths.staging_venv(plan.plan_id)).read_fingerprint()
            == plan.fingerprint_value
        )
        assert paths.marker(plan.plan_id, DEPS_INSTALLED).exists()
        assert builder.is_complete(plugin, plan)

    @pytest.mark.parametrize(
        ("backend", "env", "expected_prefix"),
        [
            ("uv", {}, ["--offline"]),
            ("virtualenv", {}, ["--no-index"]),
            (
                "virtualenv",
                {"MELTANO_PIP_FIND_LINKS": f"/tmp/wheels{os.pathsep}/tmp/more"},
                [
                    "--no-index",
                    "--find-links",
                    "/tmp/wheels",
                    "--find-links",
                    "/tmp/more",
                ],
            ),
        ],
    )
    def test_offline_args(
        self,
        project_function: Project,
        backend: str,
        env: dict[str, str],
        expected_prefix: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(os, "environ", {**os.environ, **env})
        builder = StagingBuilder(project_function)
        with patch.object(
            project_function.settings,
            "get",
            lambda name: backend,
        ):
            result = builder._offline_args(["pkg"], offline=True)
        assert result[: len(expected_prefix)] == expected_prefix
        assert result[-1] == "pkg"


class TestExecutabilityChecker:
    @pytest.mark.parametrize(
        ("help_exit", "executable_exists", "python_exists", "should_pass"),
        [
            (0, True, True, True),
            (0, False, True, False),
            (1, True, True, False),
            (0, True, False, False),
        ],
    )
    def test_check(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
        help_exit: int,
        executable_exists: bool,
        python_exists: bool,
        should_pass: bool,
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        staged = paths.staging_venv(plan.plan_id)
        staged.mkdir(parents=True)

        # Build a real venv so bin/python actually runs; create and wait on
        # the same event loop.
        async def make_venv() -> None:
            proc = await asyncio.create_subprocess_exec(
                sys_executable(),
                "-m",
                "venv",
                str(staged),
            )
            await proc.wait()

        asyncio.run(make_venv())

        bin_dir = staged / "bin"
        if not python_exists:
            (bin_dir / "python").unlink()

        if executable_exists:
            script = bin_dir / EXECUTABLE_NAME
            script.write_text(
                f"#!/bin/sh\nif [ \"$1\" = \"--help\" ]; then exit {help_exit}; fi\n"
                "exit 0\n",
            )
            script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        checker = ExecutabilityChecker(project_function, timeout=15)
        if should_pass:
            asyncio.run(checker.check(plugin, plan))
            assert paths.marker(plan.plan_id, CHECKED).exists()
        else:
            with pytest.raises(ExecutabilityCheckError) as exc_info:
                asyncio.run(checker.check(plugin, plan))
            assert exc_info.value.instruction
            assert not paths.marker(plan.plan_id, CHECKED).exists()


def sys_executable() -> str:
    import sys

    return sys.executable
