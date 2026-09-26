from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import typing as t

import pytest
import yaml

from meltano.core.install_transaction.commit import (
    CommittedState,
    InstallCommitter,
)
from meltano.core.install_transaction.errors import (
    InstallTransactionError,
    StalePlanError,
)
from meltano.core.install_transaction.paths import InstallPaths
from meltano.core.install_transaction.plan import (
    InstallPlan,
    InstallPlanService,
)
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


def reason() -> t.Any:
    from meltano.core.plugin_install_service import PluginInstallReason

    return PluginInstallReason.INSTALL


@pytest.fixture
def plan_service(project_function: Project) -> InstallPlanService:
    return InstallPlanService(project_function)


def resolve(project: Project, service: InstallPlanService) -> tuple[ProjectPlugin, InstallPlan]:
    plugin = write_custom_plugin(project, "alpha-pkg")
    return plugin, service.resolve(plugin, reason())


def make_staged(
    project: Project,
    plugin: ProjectPlugin,
    plan: InstallPlan,
) -> InstallPaths:
    paths = InstallPaths(project, plugin)
    staged = paths.staging_venv(plan.plan_id)

    async def make_venv() -> None:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "venv",
            str(staged),
        )
        await proc.wait()

    staged.mkdir(parents=True)
    asyncio.run(make_venv())

    script = staged / "bin" / EXECUTABLE_NAME
    script.write_text(
        '#!/bin/sh\nif [ "$1" = "--help" ]; then exit 0; fi\nexit 0\n',
    )
    mode = script.stat().st_mode
    script.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    VirtualEnv(staged).write_fingerprint(plan.pip_install_args)
    return paths


class TestInstallCommitter:
    def test_commit(
        self,
        project_function: Project,
        plan_service: InstallPlanService,
    ) -> None:
        plugin, plan = resolve(project_function, plan_service)
        paths = make_staged(project_function, plugin, plan)

        committer = InstallCommitter(project_function, plan_service)
        state = committer.commit(plugin, plan)

        assert isinstance(state, CommittedState)
        assert paths.venv.is_dir()
        assert paths.venv.joinpath("bin/python").exists()
        assert VirtualEnv(paths.venv).read_fingerprint() == plan.fingerprint_value

        on_disk = json.loads(paths.state_path.read_text())
        assert on_disk["plan_id"] == plan.plan_id
        assert on_disk["lock_hash"] == plan.lock_hash
        assert on_disk["fingerprint"] == plan.fingerprint_value

        assert not paths.staging_dir(plan.plan_id).exists()
        assert not list(paths.root.glob("venv.backup-*"))

    def test_commit_stale_plan(
        self,
        project_function: Project,
        plan_service: InstallPlanService,
    ) -> None:
        plugin, plan = resolve(project_function, plan_service)
        paths = make_staged(project_function, plugin, plan)

        # Config changes after staging
        changed = write_custom_plugin(project_function, "beta-pkg")

        committer = InstallCommitter(project_function, plan_service)
        with pytest.raises(StalePlanError) as exc_info:
            committer.commit(changed, plan)
        assert exc_info.value.instruction

        assert not paths.venv.exists()
        assert not paths.state_path.exists()
        assert paths.staging_venv(plan.plan_id).exists()

    def test_commit_failure_restores_previous(
        self,
        project_function: Project,
        plan_service: InstallPlanService,
    ) -> None:
        plugin, plan_a = resolve(project_function, plan_service)
        paths = make_staged(project_function, plugin, plan_a)
        committer = InstallCommitter(project_function, plan_service)
        committer.commit(plugin, plan_a)

        # Plan B
        plugin_b = write_custom_plugin(project_function, "beta-pkg")
        plan_b = plan_service.resolve(plugin_b, reason())
        paths = make_staged(project_function, plugin_b, plan_b)

        real_replace = os.replace

        def fail_replace(*args, **kwargs):
            raise OSError("simulated state write failure")

        with pytest.raises(InstallTransactionError):
            os.replace = fail_replace
            try:
                committer.commit(plugin_b, plan_b)
            finally:
                os.replace = real_replace

        # Old version restored
        on_disk = json.loads(paths.state_path.read_text())
        assert on_disk["plan_id"] == plan_a.plan_id
        assert VirtualEnv(paths.venv).read_fingerprint() == plan_a.fingerprint_value
        assert paths.venv.joinpath("bin/python").exists()
        # Staged B returned
        assert VirtualEnv(paths.staging_venv(plan_b.plan_id)).read_fingerprint() == (
            plan_b.fingerprint_value
        )
        assert not list(paths.root.glob("venv.backup-*"))

    def test_commit_lock_contention(
        self,
        project_function: Project,
        plan_service: InstallPlanService,
    ) -> None:
        plugin, plan = resolve(project_function, plan_service)
        paths = make_staged(project_function, plugin, plan)
        paths.locks_dir.mkdir(parents=True, exist_ok=True)

        # Hold the commit lock from a separate process; POSIX fcntl locks do
        # not conflict within the same process.
        holder_code = (
            "import fasteners, sys, time; "
            "lock = fasteners.InterProcessLock(sys.argv[1]); "
            "assert lock.acquire(timeout=5); "
            "sys.stdout.write('ready\\n'); sys.stdout.flush(); "
            "time.sleep(60)"
        )

        async def run() -> None:
            holder = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                holder_code,
                str(paths.commit_lock_path),
                stdout=asyncio.subprocess.PIPE,
            )
            assert await holder.stdout.readline() == b"ready\n"
            committer = InstallCommitter(
                project_function,
                plan_service,
                lock_timeout=0,
            )
            try:
                with pytest.raises(InstallTransactionError) as exc_info:
                    committer.commit(plugin, plan)
                assert exc_info.value.instruction
            finally:
                holder.terminate()
                await holder.wait()

        asyncio.run(run())
