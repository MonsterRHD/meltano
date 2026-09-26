"""End-to-end orchestration of a single plugin install transaction."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

import structlog

from meltano.core.utils import EnvironmentVariableNotSetError
from meltano.core.venv_service import VirtualEnv

from .commit import InstallCommitter
from .errors import InstallTransactionError
from .paths import InstallPaths
from .plan import InstallPlan, InstallPlanService
from .recovery import (
    RECONCILE_REUSABLE,
    STAGE_CHECK,
    STAGE_COMMIT,
    STAGE_STAGING,
    RecoveryService,
)
from .staging import ExecutabilityChecker, StagingBuilder

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.plugin_install_service import PluginInstallReason
    from meltano.core.project import Project

STATUS_SUCCESS = "success"
STATUS_SKIPPED = "skipped"
STATUS_ERROR = "error"

logger = structlog.stdlib.get_logger(__name__)

__all__ = ["InstallTransaction", "TransactionOutcome"]


@dataclass(frozen=True)
class TransactionOutcome:
    """The result of running an install transaction."""

    status: str
    message: str | None
    plan_id: str | None


class InstallTransaction:
    """Orchestrate reconcile, build, check, and commit for one plugin."""

    def __init__(
        self,
        project: Project,
        *,
        plan_service: InstallPlanService | None = None,
        builder: StagingBuilder | None = None,
        checker: ExecutabilityChecker | None = None,
        committer: InstallCommitter | None = None,
        recovery: RecoveryService | None = None,
    ):
        """Initialize the transaction.

        Args:
            project: The Meltano project.
            plan_service: Resolves install plans.
            builder: Builds staged environments.
            checker: Verifies staged environments.
            committer: Atomically commits staged environments.
            recovery: Writes recovery records and reconciles restarts.
        """
        self.project = project
        self.plan_service = plan_service or InstallPlanService(project)
        self.builder = builder or StagingBuilder(project)
        self.checker = checker or ExecutabilityChecker(project)
        self.committer = committer or InstallCommitter(project, self.plan_service)
        self.recovery = recovery or RecoveryService(project)

    async def execute(
        self,
        plugin: ProjectPlugin,
        reason: PluginInstallReasonType,
        *,
        offline: bool = False,
        force: bool = False,
    ) -> TransactionOutcome:
        """Run the full installation transaction for the plugin.

        Args:
            plugin: The plugin to install.
            reason: The reason for installing.
            offline: Only use locked, locally available artifacts.
            force: Whether to ignore the Python version required by plugins.

        Returns:
            The transaction outcome.
        """
        paths = InstallPaths(self.project, plugin)
        previous = self.committer.load_committed_state(plugin)
        previous_plan_id = previous.plan_id if previous else None

        try:
            plan: InstallPlan = self.plan_service.resolve(
                plugin,
                reason,
                offline=offline,
            )
        except EnvironmentVariableNotSetError:
            return TransactionOutcome(
                STATUS_SKIPPED,
                "Missing environment variable",
                None,
            )
        except InstallTransactionError as err:
            return TransactionOutcome(STATUS_ERROR, str(err), None)

        try:
            reconcile = self.recovery.reconcile(plugin, plan)
        except OSError as err:
            return TransactionOutcome(STATUS_ERROR, str(err), plan.plan_id)

        # Fast path: committed state matches the current plan.
        if previous and previous.plan_id == plan.plan_id:
            committed_venv = VirtualEnv(paths.venv, python=plan.python)
            if (
                committed_venv.exec_path("python").exists()
                and committed_venv.read_fingerprint() == plan.fingerprint_value
            ):
                return TransactionOutcome(
                    STATUS_SKIPPED,
                    "Requirements have not changed",
                    plan.plan_id,
                )

        def failure(stage: str, err: InstallTransactionError) -> TransactionOutcome:
            self.recovery.record_failure(
                plugin,
                plan,
                stage=stage,
                error=str(err),
                previous_plan_id=previous_plan_id,
            )
            return TransactionOutcome(STATUS_ERROR, str(err), plan.plan_id)

        self.builder.prepare(plugin, plan)

        if reconcile.mode == RECONCILE_REUSABLE:
            logger.info(
                "Reusing complete staged environment",
                plan_id=plan.plan_id,
            )
        else:
            try:
                await self.builder.build(
                    plugin,
                    plan,
                    offline=offline,
                    force=force,
                )
            except InstallTransactionError as err:
                return failure(STAGE_STAGING, err)

            try:
                await self.checker.check(plugin, plan)
            except InstallTransactionError as err:
                return failure(STAGE_CHECK, err)

        try:
            self.committer.commit(plugin, plan, offline=offline)
        except InstallTransactionError as err:
            return failure(STAGE_COMMIT, err)

        self.recovery.supersede_for(plugin)
        return TransactionOutcome(STATUS_SUCCESS, None, plan.plan_id)
