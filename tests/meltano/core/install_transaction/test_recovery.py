from __future__ import annotations

import json
import typing as t

import pytest
import yaml

from meltano.core.install_transaction.paths import InstallPaths
from meltano.core.install_transaction.plan import (
    InstallPlan,
    InstallPlanService,
)
from meltano.core.install_transaction.recovery import (
    RECONCILE_RESUMABLE,
    RECONCILE_REUSABLE,
    RecoveryService,
)
from meltano.core.venv_service import VirtualEnv

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project


def write_custom_plugin(project: Project, pip_url: str) -> ProjectPlugin:
    payload = {
        "plugins": {
            "extractors": [
                {
                    "name": "tap-custom",
                    "namespace": "tap_custom",
                    "pip_url": pip_url,
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
def resolved(project_function: Project) -> tuple[ProjectPlugin, InstallPlan]:
    plugin = write_custom_plugin(project_function, "alpha-pkg")
    plan = InstallPlanService(project_function).resolve(plugin, reason())
    return plugin, plan


def seed_staging(
    paths: InstallPaths,
    plan: InstallPlan,
    *,
    fingerprint: bool = True,
    checked: bool = True,
) -> None:
    staging = paths.staging_dir(plan.plan_id)
    staging.mkdir(parents=True)
    paths.plan_file(plan.plan_id).write_text(json.dumps(plan.to_dict()))

    venv_path = paths.staging_venv(plan.plan_id)
    venv_path.mkdir(parents=True)
    if fingerprint:
        VirtualEnv(venv_path).write_fingerprint(plan.pip_install_args)

    if checked:
        marker = paths.marker(plan.plan_id, "checked")
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()


class TestRecoveryRecords:
    def test_record_failure(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        service = RecoveryService(project_function)

        record_1 = service.record_failure(
            plugin,
            plan,
            stage="staging",
            error="boom",
            previous_plan_id="prev-id",
        )
        record_2 = service.record_failure(
            plugin,
            plan,
            stage="check",
            error="again",
        )

        paths = InstallPaths(project_function, plugin)
        files = list(paths.recovery_root.glob("*.json"))
        assert len(files) == 2

        records = list(service.iter_records(plugin))
        assert {r.record_id for r in records} == {record_1.record_id, record_2.record_id}

        open_records = service.list_open(plugin)
        assert len(open_records) == 2
        assert all(r.status == "open" for r in records)

        first = next(r for r in records if r.record_id == record_1.record_id)
        assert first.stage == "staging"
        assert first.error == "boom"
        assert first.previous_plan_id == "prev-id"
        assert first.staging_path == str(paths.staging_dir(plan.plan_id))
        assert first.suggestion

    def test_supersede(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        service = RecoveryService(project_function)
        record = service.record_failure(
            plugin,
            plan,
            stage="staging",
            error="boom",
        )

        updated = service.supersede_for(plugin)
        assert updated == 1

        paths = InstallPaths(project_function, plugin)
        on_disk = json.loads(
            paths.recovery_record_path(record.record_id).read_text(),
        )
        assert on_disk["status"] == "superseded"
        assert paths.recovery_record_path(record.record_id).exists()
        assert service.list_open(plugin) == []


class TestReconcile:
    def test_reusable(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        seed_staging(paths, plan, checked=True)

        result = RecoveryService(project_function).reconcile(plugin, plan)
        assert result.mode == RECONCILE_REUSABLE
        assert paths.staging_dir(plan.plan_id).exists()

    def test_resumable(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        seed_staging(paths, plan, checked=False)

        result = RecoveryService(project_function).reconcile(plugin, plan)
        assert result.mode == RECONCILE_RESUMABLE
        assert paths.staging_dir(plan.plan_id).exists()

    def test_stale_staging_removed(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)

        other_plugin = write_custom_plugin(project_function, "beta-pkg")
        other_plan = InstallPlanService(project_function).resolve(
            other_plugin,
            reason(),
        )
        seed_staging(paths, other_plan)

        result = RecoveryService(project_function).reconcile(plugin, plan)
        assert result.mode == "none"
        assert not paths.staging_dir(other_plan.plan_id).exists()

    def test_corrupt_staging_removed(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        bad = paths.staging_dir("corrupt")
        bad.mkdir(parents=True)
        paths.plan_file("corrupt").write_text("not json")

        RecoveryService(project_function).reconcile(plugin, plan)
        assert not bad.exists()

    def test_interrupted_swap_restore(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        backup = paths.venv_backup("oldplan")
        backup.mkdir(parents=True)
        sentinel = backup / "sentinel"
        sentinel.write_text("old")

        RecoveryService(project_function).reconcile(plugin, plan)
        assert paths.venv.is_dir()
        assert (paths.venv / "sentinel").read_text() == "old"
        assert not backup.exists()

    def test_completed_swap_removes_backup(
        self,
        project_function: Project,
        resolved: tuple[ProjectPlugin, InstallPlan],
    ) -> None:
        plugin, plan = resolved
        paths = InstallPaths(project_function, plugin)
        paths.venv.mkdir(parents=True)
        backup = paths.venv_backup("oldplan")
        backup.mkdir(parents=True)

        RecoveryService(project_function).reconcile(plugin, plan)
        assert paths.venv.is_dir()
        assert not backup.exists()
