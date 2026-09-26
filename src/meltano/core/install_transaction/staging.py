"""Staging environment construction and executability checks."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import typing as t

from meltano.core.error import AsyncSubprocessError
from meltano.core.venv_service import VirtualEnv, VirtualEnvService

from .errors import (
    ExecutabilityCheckError,
    OfflineUnavailableError,
    StagingBuildError,
)
from .paths import InstallPaths
from .plan import InstallPlan

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project

VENV_CREATED = "venv-created"
DEPS_INSTALLED = "deps-installed"
CHECKED = "checked"

PIP_FIND_LINKS_ENV = "MELTANO_PIP_FIND_LINKS"
DEFAULT_CHECK_TIMEOUT = 30

__all__ = ["ExecutabilityChecker", "StagingBuilder"]


class StagingBuilder:
    """Build plugin virtual environments inside isolated staging directories."""

    def __init__(self, project: Project):
        """Initialize the staging builder.

        Args:
            project: The Meltano project.
        """
        self.project = project

    def prepare(self, plugin: ProjectPlugin, plan: InstallPlan) -> InstallPaths:
        """Create the staging directory and persist the resolved plan.

        Args:
            plugin: The plugin being installed.
            plan: The resolved install plan.

        Returns:
            The transaction paths.
        """
        paths = InstallPaths(self.project, plugin)
        staging_dir = paths.staging_dir(plan.plan_id)
        staging_dir.mkdir(parents=True, exist_ok=True)
        paths.plan_file(plan.plan_id).write_text(
            json.dumps(plan.to_dict(), indent=2) + "\n",
        )
        return paths

    async def build(
        self,
        plugin: ProjectPlugin,
        plan: InstallPlan,
        *,
        offline: bool = False,
        force: bool = False,
    ) -> None:
        """Create the virtual environment and install dependencies in staging.

        Args:
            plugin: The plugin being installed.
            plan: The resolved install plan.
            offline: Restrict installation to locked, locally available artifacts.
            force: Whether to ignore the Python version required by plugins.

        Raises:
            StagingBuildError: Dependency installation failed.
            OfflineUnavailableError: Installation failed while offline.
        """
        paths = InstallPaths(self.project, plugin)
        venv_path = paths.staging_venv(plan.plan_id)
        service = VirtualEnvService.from_plugin(
            self.project,
            plugin,
            venv_path=venv_path,
        )

        venv_marker = paths.marker(plan.plan_id, VENV_CREATED)
        if not venv_marker.exists():
            await service.create()
            venv_marker.parent.mkdir(parents=True, exist_ok=True)
            venv_marker.touch()

        service.clean_run_files()

        install_args = self._offline_args(
            list(plan.pip_install_args),
            offline=offline,
        )
        try:
            await service.pip_install(
                install_args,
                force=force,
                env=self._subprocess_env(),
            )
        except AsyncSubprocessError as err:
            error_cls = (
                OfflineUnavailableError
                if offline
                else StagingBuildError
            )
            descriptor = f"{plugin.type.descriptor} '{plugin.name}'"
            if offline:
                raise error_cls(
                    reason=(
                        f"Failed to install {descriptor} offline: a locked artifact"
                        " is not available locally"
                    ),
                    instruction=(
                        "Retry while online, or make the locked wheels available in "
                        f"${PIP_FIND_LINKS_ENV}"
                    ),
                ) from err
            raise error_cls(
                reason=f"Failed to install {descriptor} in the staging environment",
                instruction=(
                    "Review the pip install log, fix the dependency issue, and retry"
                ),
            ) from err

        service.venv.write_fingerprint(plan.pip_install_args)

        deps_marker = paths.marker(plan.plan_id, DEPS_INSTALLED)
        deps_marker.parent.mkdir(parents=True, exist_ok=True)
        deps_marker.touch()

    def is_complete(self, plugin: ProjectPlugin, plan: InstallPlan) -> bool:
        """Return whether the staged environment for the plan is fully built."""
        paths = InstallPaths(self.project, plugin)
        if not paths.marker(plan.plan_id, DEPS_INSTALLED).exists():
            return False
        venv = VirtualEnv(
            paths.staging_venv(plan.plan_id),
            python=plan.python,
        )
        return venv.read_fingerprint() == plan.fingerprint_value

    def is_verified(self, plugin: ProjectPlugin, plan: InstallPlan) -> bool:
        """Return whether the staged environment previously passed all checks."""
        paths = InstallPaths(self.project, plugin)
        return paths.marker(plan.plan_id, CHECKED).exists()

    def _offline_args(self, args: list[str], *, offline: bool) -> list[str]:
        if not offline:
            return args

        backend = self.project.settings.get("venv.backend")
        if backend == "uv":
            return ["--offline", *args]

        offline_args = ["--no-index"]
        if find_links := os.environ.get(PIP_FIND_LINKS_ENV):
            for path in find_links.split(os.pathsep):
                if path:
                    offline_args.extend(["--find-links", path])
        return [*offline_args, *args]

    def _subprocess_env(self) -> dict[str, str]:
        return {
            **os.environ,
            **self.project.dotenv_env,
            **self.project.meltano.env,
        }


class ExecutabilityChecker:
    """Verify a staged environment is executable before it is committed."""

    def __init__(self, project: Project, *, timeout: int = DEFAULT_CHECK_TIMEOUT):
        """Initialize the checker.

        Args:
            project: The Meltano project.
            timeout: Per-command timeout in seconds.
        """
        self.project = project
        self.timeout = timeout

    async def check(self, plugin: ProjectPlugin, plan: InstallPlan) -> None:
        """Run the executability checks for the staged environment.

        Args:
            plugin: The plugin being installed.
            plan: The resolved install plan.

        Raises:
            ExecutabilityCheckError: Any check failed.
        """
        paths = InstallPaths(self.project, plugin)
        venv = VirtualEnv(
            paths.staging_venv(plan.plan_id),
            python=plan.python,
        )

        python = venv.exec_path("python")
        if not python.exists():
            raise ExecutabilityCheckError(
                reason=(
                    f"Staged Python interpreter does not exist at {python}"
                ),
                instruction="Virtual environment creation may have failed; retry the install",
            )
        try:
            await self._run((str(python), "-c", "import sys"))
        except ExecutabilityCheckError as err:
            raise ExecutabilityCheckError(
                reason=f"Staged Python interpreter failed to run: {err.reason}",
                instruction="The virtual environment may be corrupted; retry the install",
            ) from err

        executable = venv.exec_path(plugin.executable)
        if not executable.exists():
            raise ExecutabilityCheckError(
                reason=(
                    f"Expected executable '{plugin.executable}' was not found in the"
                    f" staged environment ({executable})"
                ),
                instruction=(
                    "Verify the plugin's 'executable' and 'pip_url', then retry the"
                    " install"
                ),
            )
        if not os.access(executable, os.X_OK):
            raise ExecutabilityCheckError(
                reason=(
                    f"Staged executable '{executable}' exists but is not executable"
                ),
                instruction="Check file permissions in the staging directory and retry",
            )

        try:
            await self._run((str(executable), "--help"))
        except ExecutabilityCheckError as err:
            raise ExecutabilityCheckError(
                reason=(
                    f"Plugin executable '{plugin.executable}' failed the '--help'"
                    f" probe: {err.reason}"
                ),
                instruction=(
                    "The install may be incomplete, or the plugin does not support"
                    " '--help'; inspect the staged environment"
                ),
            ) from err

        marker = paths.marker(plan.plan_id, CHECKED)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()

    async def _run(self, argv: tuple[str, ...]) -> str:
        """Run a command, enforcing a timeout and zero exit code."""
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await asyncio.wait_for(proc.wait(), self.timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise ExecutabilityCheckError(
                reason=(
                    f"Command timed out after {self.timeout}s: {shlex.join(argv)}"
                ),
                instruction="The plugin may be unresponsive; retry or report the issue",
            )

        output = b""
        if proc.stdout:
            output = await proc.stdout.read()
        text = output.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            tail = text[-2000:].strip()
            raise ExecutabilityCheckError(
                reason=(
                    f"Command exited with code {proc.returncode}: {shlex.join(argv)}"
                    + (f"\n{tail}" if tail else "")
                ),
                instruction="Inspect the command output above and correct the issue",
            )
        return text
