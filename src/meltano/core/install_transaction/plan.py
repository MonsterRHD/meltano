"""Resolved, identity-bearing plugin install plans."""

from __future__ import annotations

import hashlib
import json
import os
import typing as t
from dataclasses import dataclass
from datetime import UTC, datetime

from meltano.core.plugin.error import PluginNotFoundError as PluginNotFound
from meltano.core.plugin_lock_service import PluginLockService
from meltano.core.utils import get_meltano_version
from meltano.core.venv_service import fingerprint

from .errors import InstallResolutionError

if t.TYPE_CHECKING:
    import sys

    from collections.abc import Mapping

    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.plugin_install_service import PluginInstallReason
    from meltano.core.project import Project

    if sys.version_info >= (3, 11):
        from typing import Self  # noqa: ICN003
    else:
        from typing_extensions import Self

OFFLINE_ENV_VAR = "MELTANO_OFFLINE"
_OFFLINE_TRUTHY = frozenset({"1", "true", "yes", "on"})

__all__ = ["InstallPlan", "InstallPlanService", "offline_from_env", "truthy_offline"]


def truthy_offline(value: str | None) -> bool:
    """Return whether the given string enables offline mode.

    Truthy values (case-insensitive): 1, true, yes, on.
    """
    if value is None:
        return False
    return value.strip().lower() in _OFFLINE_TRUTHY


def offline_from_env(env: Mapping[str, str] | None = None) -> bool:
    """Read the offline flag from the `MELTANO_OFFLINE` environment variable."""
    source = env if env is not None else os.environ
    return truthy_offline(source.get(OFFLINE_ENV_VAR))


def _sha256_json(payload: object) -> str:
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(content.encode()).hexdigest()


@dataclass(frozen=True)
class InstallPlan:
    """A complete, identity-bearing plan for installing one plugin.

    The `plan_id` is deterministic: identical project/lock inputs always yield
    the same plan ID, while any change to the pip args, variant, lock content,
    or Python interpreter changes the ID.
    """

    plan_id: str
    plugin_type: str
    plugin_name: str
    plugin_dir_name: str
    variant: str | None
    namespace: str | None
    python: str
    pip_install_args: tuple[str, ...]
    lock_content: dict[str, t.Any]
    lock_hash: str
    reason: str
    created_at: str
    meltano_version: str

    @property
    def fingerprint_value(self) -> str:
        """The virtual environment fingerprint for this plan."""
        return fingerprint(self.pip_install_args, self.python)

    def to_dict(self) -> dict[str, t.Any]:
        """Serialize the plan to a JSON-compatible dictionary."""
        return {
            "plan_id": self.plan_id,
            "plugin_type": self.plugin_type,
            "plugin_name": self.plugin_name,
            "plugin_dir_name": self.plugin_dir_name,
            "variant": self.variant,
            "namespace": self.namespace,
            "python": self.python,
            "pip_install_args": list(self.pip_install_args),
            "lock_content": self.lock_content,
            "lock_hash": self.lock_hash,
            "reason": self.reason,
            "created_at": self.created_at,
            "meltano_version": self.meltano_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, t.Any]) -> Self:
        """Reconstruct a plan from a dictionary produced by :meth:`to_dict`."""
        return cls(
            plan_id=data["plan_id"],
            plugin_type=data["plugin_type"],
            plugin_name=data["plugin_name"],
            plugin_dir_name=data["plugin_dir_name"],
            variant=data.get("variant"),
            namespace=data.get("namespace"),
            python=data["python"],
            pip_install_args=tuple(data["pip_install_args"]),
            lock_content=dict(data["lock_content"]),
            lock_hash=data["lock_hash"],
            reason=data["reason"],
            created_at=data["created_at"],
            meltano_version=data["meltano_version"],
        )


class InstallPlanService:
    """Resolve install plans from the current project and lockfiles."""

    def __init__(self, project: Project):
        """Initialize the plan service.

        Args:
            project: The Meltano project.
        """
        self.project = project

    def resolve(
        self,
        plugin: ProjectPlugin,
        reason: PluginInstallReason,
        *,
        offline: bool = False,
    ) -> InstallPlan:
        """Resolve the install plan for the given plugin.

        Args:
            plugin: The plugin to install.
            reason: The reason for installing.
            offline: When true, never fetch definitions from the Hub; a missing
                lockfile is a resolution error.

        Returns:
            The resolved install plan.

        Raises:
            InstallResolutionError: The lockfile is missing in offline mode.
        """
        # Lazy imports avoid a circular import with `plugin_install_service`.
        from meltano.core.plugin_install_service import (
            PluginInstallService,
            get_pip_install_args,
        )
        from meltano.core.utils import EnvVarMissingBehavior

        install_service = PluginInstallService(self.project)
        install_env = install_service.plugin_installation_env(plugin)
        if reason.value == "auto":
            pip_install_args = get_pip_install_args(
                self.project,
                plugin,
                install_env,
                if_missing=EnvVarMissingBehavior.raise_exception,
            )
        else:
            pip_install_args = get_pip_install_args(
                self.project,
                plugin,
                install_env,
            )

        python = (
            plugin.python
            or self.project.settings.get("python")
            or self.project.python_version
        )

        lock_content = self._lock_content(plugin, offline=offline)
        lock_hash = _sha256_json(lock_content)

        plan_id = _sha256_json(
            {
                "plugin_type": plugin.type.value,
                "plugin_dir_name": plugin.plugin_dir_name,
                "variant": plugin.variant,
                "pip_install_args": sorted(set(pip_install_args)),
                "python": python,
                "lock_hash": lock_hash,
            },
        )

        return InstallPlan(
            plan_id=plan_id,
            plugin_type=plugin.type.value,
            plugin_name=plugin.name,
            plugin_dir_name=plugin.plugin_dir_name,
            variant=plugin.variant,
            namespace=plugin.namespace,
            python=python,
            pip_install_args=tuple(pip_install_args),
            lock_content=lock_content,
            lock_hash=lock_hash,
            reason=reason.value,
            created_at=datetime.now(UTC).isoformat(),
            meltano_version=get_meltano_version(),
        )

    def _lock_content(
        self,
        plugin: ProjectPlugin,
        *,
        offline: bool,
    ) -> dict[str, t.Any]:
        lock_service = PluginLockService(self.project)
        plugin_name, variant_name = self._lock_identity(plugin)
        lock_path = lock_service.lock_path(
            plugin_type=plugin.type,
            plugin_name=plugin_name,
            variant_name=variant_name,
        )

        if lock_path.exists():
            with lock_path.open() as lockfile:
                return json.load(lockfile)

        if plugin.is_custom():
            # Custom plugins carry their definition locally in the project
            # files, so it is available even offline.
            return lock_service.get_standalone_data(plugin)

        if offline:
            descriptor = f"{plugin.type.descriptor} '{plugin.name}'"
            raise InstallResolutionError(
                reason=(
                    f"Cannot install {descriptor} offline: no lockfile found at "
                    f"{lock_path.relative_to(self.project.root)}"
                ),
                instruction=(
                    "Run 'meltano lock' / 'meltano install' while online to create "
                    "the lockfile, or choose a different environment"
                ),
            )

        content, _metadata = lock_service.load_content(
            plugin_type=plugin.type,
            plugin_name=plugin_name,
            variant_name=variant_name,
        )
        return content

    def _lock_identity(
        self,
        plugin: ProjectPlugin,
    ) -> tuple[str, str | None]:
        """Return the (name, variant) of the root plugin in the inheritance chain.

        Plugins sharing a virtual environment also share one locked definition.
        """
        current = plugin
        seen: set[str] = set()
        while current.inherit_from:
            parent_name = current.inherit_from
            if parent_name in seen:
                raise InstallResolutionError(
                    reason=(
                        f"Plugin inheritance is cyclic at {parent_name!r}; cannot "
                        "resolve a lockfile"
                    ),
                    instruction="Fix the 'inherit_from' chain in meltano.yml",
                )
            seen.add(parent_name)
            try:
                parent = self.project.plugins.find_plugin(
                    parent_name,
                    plugin_type=plugin.type,
                )
            except PluginNotFound:
                break
            current = parent
        return current.inherit_from or current.name, current.variant
