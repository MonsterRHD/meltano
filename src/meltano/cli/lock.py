"""Lock command."""

from __future__ import annotations

import typing as t

import click
import structlog

from meltano.cli.params import PluginTypeArg, pass_project
from meltano.cli.utils import CliError, PartialInstrumentedCmd
from meltano.core.error import ProjectReadonly
from meltano.core.lock_snapshot_service import (
    ENTRY_CUSTOM,
    ENTRY_INHERITED,
    RESULT_FAILED,
    RESULT_LOCKED,
    RESULT_REUSED,
    LockSnapshotService,
)
from meltano.core.plugin.base import PluginRef, PluginType
from meltano.core.plugin_lock_service import (
    LockfileAlreadyExistsError,
    PluginLockService,
)
from meltano.core.tracking.contexts import CliEvent, PluginsTrackingContext

if t.TYPE_CHECKING:
    from meltano.core.lock_snapshot_service import ItemResult
    from meltano.core.plugin.project_plugin import ProjectPlugin
    from meltano.core.project import Project
    from meltano.core.tracking import Tracker

__all__ = ["lock"]
logger = structlog.get_logger(__name__)


@click.command(cls=PartialInstrumentedCmd, short_help="Lock plugin definitions.")
@click.option(
    "--plugin-type",
    type=PluginTypeArg(),
    help="Lock only the plugins of the given type.",
)
@click.argument("plugin_name", nargs=-1, required=False)
@click.option("--update", "-u", is_flag=True, help="Update the lock file.")
@click.option(
    "--batch",
    is_flag=True,
    help=(
        "Resolve all matching plugins and publish one atomic, versioned lock snapshot."
    ),
)
@click.pass_context
@pass_project()
def lock(
    project: Project,
    ctx: click.Context,
    *,
    plugin_type: PluginType | None,
    plugin_name: tuple[str, ...],
    update: bool,
    batch: bool,
) -> None:
    """Lock plugin definitions.

    \b
    Read more at https://docs.meltano.com/reference/command-line-interface#lock
    """  # noqa: D301
    tracker: Tracker = ctx.obj["tracker"]

    if project.readonly:
        tracker.track_command_event(CliEvent.aborted)
        raise ProjectReadonly

    lock_service = PluginLockService(project)

    try:
        # Make it a list so source preference is not lazily evaluated.
        # Pass ensure_parent=False to avoid fetching from Hub when no lockfile
        # exists, since we'll be fetching explicitly later anyway.
        plugins = list(project.plugins.plugins(ensure_parent=False))
    except Exception:
        tracker.track_command_event(CliEvent.aborted)
        raise

    if plugin_name:
        plugins = [plugin for plugin in plugins if plugin.name in plugin_name]

    if plugin_type is not None:
        plugins = [plugin for plugin in plugins if plugin.type == plugin_type]

    tracked_plugins: list[tuple[PluginRef, str | None]] = []

    if not plugins:
        tracker.track_command_event(CliEvent.aborted)
        errmsg = "No matching plugin(s) found"
        raise CliError(errmsg)

    if batch:
        results = _run_lock_batch(project, plugins, update=update)

        for result in results:
            descriptor = result.descriptor
            plugin_ref_name = descriptor.partition(" ")[2]
            plugin_type_value = result.key.split("/", 1)[0]

            if result.kind == ENTRY_CUSTOM:
                logger.warning("%s is a custom plugin", descriptor.capitalize())
            elif result.kind == ENTRY_INHERITED:
                logger.warning(
                    "%s is an inherited plugin",
                    descriptor.capitalize(),
                )
            elif result.status == RESULT_REUSED:
                logger.info("Reused cached definition for %s", descriptor)
                tracked_plugins.append(
                    (
                        PluginRef(PluginType(plugin_type_value), plugin_ref_name),
                        result.variant,
                    ),
                )
            elif result.status == RESULT_LOCKED:
                logger.info("Locked definition for %s", descriptor)
                tracked_plugins.append(
                    (
                        PluginRef(PluginType(plugin_type_value), plugin_ref_name),
                        result.variant,
                    ),
                )

        failed_results = [
            result for result in results if result.status == RESULT_FAILED
        ]
        for result in failed_results:
            logger.error(
                "Failed to lock %s: %s",
                result.descriptor,
                result.error,
            )

        if failed_results:
            tracker.track_command_event(CliEvent.aborted)
            errmsg = (
                f"Failed to lock {len(failed_results)} plugin(s); the "
                "previous lock snapshot was retained. Fix the failing "
                "plugins and re-run: successful resolutions are reused."
            )
            raise CliError(errmsg)

        tracker.add_contexts(PluginsTrackingContext(tracked_plugins))
        tracker.track_command_event(CliEvent.completed)
        return

    logger.info("Locking %d plugin(s)...", len(plugins))
    for plugin in plugins:
        descriptor = f"{plugin.type.descriptor} {plugin.name}"
        if plugin.is_custom():
            logger.warning("%s is a custom plugin", descriptor.capitalize())
        elif plugin.inherit_from is not None:
            logger.warning("%s is an inherited plugin", descriptor.capitalize())
        else:
            plugin.parent = None
            try:
                lock_service.save(plugin, exists_ok=update, fetch_from_hub=True)
            except LockfileAlreadyExistsError as err:
                relative_path = err.path.relative_to(project.root)
                logger.error(  # noqa: TRY400
                    "Lockfile exists for %s at %s",
                    descriptor,
                    relative_path,
                )
                continue

            tracked_plugins.append((plugin, None))
            logger.info("Locked definition for %s", descriptor)

    tracker.add_contexts(PluginsTrackingContext(tracked_plugins))
    tracker.track_command_event(CliEvent.completed)


def _run_lock_batch(
    project: Project,
    plugins: list[ProjectPlugin],
    *,
    update: bool,
) -> list[ItemResult]:
    """Run one lock batch and publish its snapshot.

    Args:
        project: The Meltano project.
        plugins: The resolved batch plugins.
        update: Whether replacing a published snapshot is allowed.

    Returns:
        One result per plugin.
    """
    snapshot_service = LockSnapshotService(project)

    # Finish or remove interrupted batches before starting this one.
    snapshot_service.recover()

    snapshot_service.ensure_can_publish(update=update)

    staging = snapshot_service.begin_batch(snapshot_service.read_manifest())
    for plugin in plugins:
        snapshot_service.add_item(staging, plugin)

    results = snapshot_service.prepare(staging)

    if any(result.status == RESULT_FAILED for result in results):
        return results

    snapshot_service.assemble(staging)
    manifest = snapshot_service.publish(staging)

    logger.info(
        "Published lock snapshot %s with %d plugin(s)",
        manifest.snapshot_id,
        len(
            [
                result
                for result in results
                if result.status in {RESULT_LOCKED, RESULT_REUSED}
            ],
        ),
    )
    return results
