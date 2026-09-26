from __future__ import annotations

import json
import typing as t
from unittest.mock import patch

import pytest
import yaml

from meltano.core.install_transaction.errors import InstallResolutionError
from meltano.core.install_transaction.plan import (
    InstallPlan,
    InstallPlanService,
    offline_from_env,
    truthy_offline,
)
from meltano.core.plugin import PluginType
from meltano.core.project_plugins_service import PluginAlreadyAddedException

if t.TYPE_CHECKING:
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project


def write_custom_plugin(project: Project, pip_url: str, **extra) -> ProjectPlugin:
    payload = {
        "plugins": {
            "extractors": [
                {
                    "name": "tap-custom",
                    "namespace": "tap_custom",
                    "pip_url": pip_url,
                    **extra,
                },
            ],
        },
    }
    with project.meltanofile.open("w") as file:
        file.write(yaml.dump(payload))
    project.refresh()
    return next(project.plugins.plugins())


@pytest.fixture
def service(project: Project) -> InstallPlanService:
    return InstallPlanService(project)


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


class TestInstallPlanService:
    def test_resolve_deterministic(
        self,
        project_function: Project,
        service: InstallPlanService,
    ) -> None:
        # service is bound to the class project; build a fresh one.
        plan_service = InstallPlanService(project_function)
        plugin = write_custom_plugin(project_function, "alpha-pkg")

        plan_1 = plan_service.resolve(plugin, _reason())
        plan_2 = plan_service.resolve(plugin, _reason())
        assert plan_1.plan_id == plan_2.plan_id
        assert plan_1.lock_hash == plan_2.lock_hash

    @pytest.mark.parametrize(
        ("mutation", "value"),
        [
            ("pip_url", "beta-pkg"),
            ("variant", "other-variant"),
        ],
    )
    def test_plan_id_changes_with_definition(
        self,
        project_function: Project,
        mutation: str,
        value: str,
    ) -> None:
        plan_service = InstallPlanService(project_function)
        plugin = write_custom_plugin(project_function, "alpha-pkg")
        before = plan_service.resolve(plugin, _reason())

        if mutation == "pip_url":
            plugin = write_custom_plugin(project_function, value)
        else:
            plugin = write_custom_plugin(project_function, "alpha-pkg", variant=value)

        after = plan_service.resolve(plugin, _reason())
        assert before.plan_id != after.plan_id

    def test_plan_id_changes_with_python(
        self,
        project_function: Project,
    ) -> None:
        plan_service = InstallPlanService(project_function)
        plugin = write_custom_plugin(project_function, "alpha-pkg")
        before = plan_service.resolve(plugin, _reason())

        plugin.python = "/usr/bin/python3.9"
        after = plan_service.resolve(plugin, _reason())
        assert before.plan_id != after.plan_id

    def test_plan_id_changes_with_lock(
        self,
        project: Project,
        service: InstallPlanService,
        tap: ProjectPlugin,
    ) -> None:
        from meltano.core.plugin_lock_service import PluginLockService

        lock_path = PluginLockService(project).lock_path(
            plugin_type=tap.type,
            plugin_name=tap.name,
            variant_name=tap.variant,
        )
        original = lock_path.read_text()
        before = service.resolve(tap, _reason())
        try:
            content = json.loads(original)
            content["extra-lock-field"] = "changed"
            lock_path.write_text(json.dumps(content))
            after = service.resolve(tap, _reason())
            assert before.plan_id != after.plan_id
        finally:
            lock_path.write_text(original)

    def test_offline_resolve_no_hub_for_custom(
        self,
        project_function: Project,
    ) -> None:
        plan_service = InstallPlanService(project_function)
        plugin = write_custom_plugin(project_function, "alpha-pkg")
        with patch(
            "meltano.core.plugin_lock_service.PluginLockService.load_content",
            side_effect=AssertionError("Hub must not be contacted offline"),
        ):
            plan = plan_service.resolve(plugin, _reason(), offline=True)
        assert isinstance(plan, InstallPlan)

    def test_offline_missing_lock_raises(
        self,
        project: Project,
        service: InstallPlanService,
        tap: ProjectPlugin,
    ) -> None:
        from meltano.core.plugin_lock_service import PluginLockService

        lock_path = PluginLockService(project).lock_path(
            plugin_type=tap.type,
            plugin_name=tap.name,
            variant_name=tap.variant,
        )
        moved = lock_path.with_suffix(".lock.tmp")
        lock_path.rename(moved)
        try:
            with (
                patch(
                    "meltano.core.plugin_lock_service.PluginLockService.load_content",
                    side_effect=AssertionError("Hub must not be contacted offline"),
                ),
                pytest.raises(InstallResolutionError) as exc_info,
            ):
                service.resolve(tap, _reason(), offline=True)
            assert exc_info.value.instruction
        finally:
            moved.rename(lock_path)

    def test_plan_json_round_trip(
        self,
        project_function: Project,
    ) -> None:
        plan_service = InstallPlanService(project_function)
        plugin = write_custom_plugin(project_function, "alpha-pkg extra-dep")
        plan = plan_service.resolve(plugin, _reason())

        rebuilt = InstallPlan.from_dict(json.loads(json.dumps(plan.to_dict())))
        assert rebuilt == plan
        assert rebuilt.fingerprint_value == plan.fingerprint_value

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("1", True),
            ("true", True),
            ("TRUE", True),
            ("yes", True),
            ("on", True),
            ("0", False),
            ("false", False),
            ("", False),
            (None, False),
        ],
    )
    def test_truthy_offline(self, value: str | None, expected: bool) -> None:
        assert truthy_offline(value) is expected

    def test_offline_from_env(self) -> None:
        assert offline_from_env({"MELTANO_OFFLINE": "yes"}) is True
        assert offline_from_env({"MELTANO_OFFLINE": "no"}) is False
        assert offline_from_env({}) is False


def _reason() -> t.Any:
    from meltano.core.plugin_install_service import PluginInstallReason

    return PluginInstallReason.INSTALL
