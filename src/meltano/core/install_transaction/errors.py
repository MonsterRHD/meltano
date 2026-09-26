"""Errors raised during a plugin install transaction."""

from __future__ import annotations

from meltano.core.error import MeltanoError

__all__ = [
    "ExecutabilityCheckError",
    "InstallResolutionError",
    "InstallTransactionError",
    "OfflineUnavailableError",
    "StalePlanError",
    "StagingBuildError",
]


class InstallTransactionError(MeltanoError):
    """Base class for all install transaction failures."""


class InstallResolutionError(InstallTransactionError):
    """Raised when an install plan cannot be resolved."""


class StalePlanError(InstallTransactionError):
    """Raised when a staged plan no longer matches the current project and lock."""


class StagingBuildError(InstallTransactionError):
    """Raised when dependency installation in the staging environment fails."""


class ExecutabilityCheckError(InstallTransactionError):
    """Raised when the staged environment fails executability checks."""


class OfflineUnavailableError(InstallTransactionError):
    """Raised in offline mode when a locked artifact is not locally available."""
