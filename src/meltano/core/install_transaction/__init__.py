"""Transactional plugin installation primitives.

This package implements an atomic, recoverable plugin installation flow:

1. Resolve an :class:`~meltano.core.install_transaction.plan.InstallPlan` with a
   deterministic identity from the current project and lockfile.
2. Build the virtual environment in an isolated staging directory and verify
   executability.
3. Atomically swap the staged environment into place together with the
   committed state record.

Any failure preserves the last known runnable version and writes an
identifiable recovery record.
"""

from __future__ import annotations
