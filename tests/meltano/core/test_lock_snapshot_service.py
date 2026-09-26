"""Tests for the lock snapshot service."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import typing as t

import fasteners
import pytest

from meltano.core.hub.client import MeltanoHubService
from meltano.core.lock_snapshot_service import (
    ENTRY_INHERITED,
    ENTRY_LEGACY,
    ENTRY_LOCKED,
    ITEM_FAILED,
    ITEM_SUCCESS,
    PROVENANCE_HUB,
    PROVENANCE_LOCAL,
    RESULT_FAILED,
    RESULT_LOCKED,
    RESULT_REUSED,
    STATE_RESOLVING,
    STATE_STAGED,
    DefinitionProvenance,
    LockSnapshotManifest,
    LockSnapshotService,
    SnapshotConflictError,
    SnapshotEntry,
    SnapshotError,
    SnapshotExistsError,
    Staging,
    StagingItem,
    compute_fingerprint,
)
from meltano.core.plugin.base import PluginType, StandalonePlugin
from meltano.core.plugin.project_plugin import ProjectPlugin
from meltano.core.plugin_lock_service import PluginLockService

if t.TYPE_CHECKING:
    from meltano.core.project import Project


@pytest.fixture(autouse=True)
def _clear_locks(project: Project):
    """Keep tests independent under randomized test ordering."""
    staging_root = project.dirs.plugin_lock_staging_dir()
    if staging_root.exists():
        shutil.rmtree(staging_root)

    pointer = project.dirs.plugin_lock_snapshot_pointer()
    if pointer.exists():
        pointer.unlink()

    snapshots = project.dirs.plugin_lock_snapshots_dir()
    if snapshots.exists():
        shutil.rmtree(snapshots)


@pytest.fixture
def subject(project: Project) -> LockSnapshotService:
    return LockSnapshotService(project)


class TestSnapshotPaths:
    def test_pointer_path(self, subject: LockSnapshotService, project: Project) -> None:
        assert subject.pointer_path == project.root / "plugins" / "lock.snapshot.json"

    def test_snapshot_dir(self, subject: LockSnapshotService, project: Project) -> None:
        assert (
            subject.snapshot_dir("abc")
            == project.root / "plugins" / "snapshots" / "abc"
        )

    def test_staging_dir(self, subject: LockSnapshotService, project: Project) -> None:
        assert (
            subject.staging_dir("abc")
            == project.sys_dir_root / "lock" / "staging" / "abc"
        )

    def test_batch_lock_path(
        self,
        subject: LockSnapshotService,
        project: Project,
    ) -> None:
        assert (
            subject.batch_lock_path
            == project.sys_dir_root / "run" / "lock-snapshot.lock"
        )

    def test_read_manifest_absent(self, subject: LockSnapshotService) -> None:
        assert subject.read_manifest() is None


class TestFingerprint:
    @pytest.mark.parametrize(
        "kwargs",
        (
            {
                "plugin_type": "extractors",
                "plugin_name": "tap-mock",
                "variant": "meltano",
                "hub_origin": "https://hub.meltano.com/meltano/api/v1",
            },
        ),
    )
    def test_stable(self, kwargs: dict[str, t.Any]) -> None:
        assert compute_fingerprint(**kwargs) == compute_fingerprint(**kwargs)

    @pytest.mark.parametrize(
        "changed",
        (
            {"plugin_type": "loaders"},
            {"plugin_name": "tap-other"},
            {"variant": "singer-io"},
            {"hub_origin": "https://example.com/api"},
        ),
    )
    def test_inputs_change_fingerprint(
        self,
        changed: dict[str, t.Any],
    ) -> None:
        base = {
            "plugin_type": "extractors",
            "plugin_name": "tap-mock",
            "variant": "meltano",
            "hub_origin": "https://hub.meltano.com/meltano/api/v1",
        }
        assert compute_fingerprint(**base) != compute_fingerprint(
            **{**base, **changed},
        )

    def test_local_definition_changes_fingerprint(self) -> None:
        kwargs = {
            "plugin_type": "extractors",
            "plugin_name": "tap-mock",
            "variant": None,
            "hub_origin": "https://hub.meltano.com/meltano/api/v1",
        }
        fp1 = compute_fingerprint(**kwargs, local_definition={"a": 1})
        fp2 = compute_fingerprint(**kwargs, local_definition={"a": 2})
        assert fp1 != fp2


class TestRoundTrip:
    def test_provenance(self) -> None:
        ref = (
            "https://hub.meltano.com/meltano/api/v1/plugins/extractors/"
            "tap-mock--meltano"
        )
        provenance = DefinitionProvenance(
            kind="hub",
            origin="https://hub.meltano.com/meltano/api/v1",
            ref=ref,
        )
        assert DefinitionProvenance.parse(provenance.canonical()) == provenance

    @pytest.mark.parametrize(
        "entry",
        (
            SnapshotEntry(
                type="extractors",
                name="tap-mock",
                kind="locked",
                variant="meltano",
                file="plugins/snapshots/abc/extractors/tap-mock--meltano.lock",
                provenance=DefinitionProvenance(
                    "hub",
                    "https://hub.meltano.com/meltano/api/v1",
                ),
                is_default_variant=True,
                is_deprecated=False,
            ),
            SnapshotEntry(
                type="extractors",
                name="tap-mock-inherited",
                kind="inherited",
                inherit_from="tap-mock",
            ),
            SnapshotEntry(type="extractors", name="tap-custom", kind="custom"),
            SnapshotEntry(
                type="extractors",
                name="tap-legacy",
                kind="legacy",
                variant=None,
                file="plugins/snapshots/abc/extractors/tap-legacy.lock",
            ),
        ),
    )
    def test_snapshot_entry(self, entry: SnapshotEntry) -> None:
        assert SnapshotEntry.parse(entry.canonical()) == entry

    def test_manifest(self) -> None:
        manifest = LockSnapshotManifest(
            snapshot_id="abc",
            project_version="0" * 64,
            environment="prod",
            created_at="2026-09-25T00:00:00+00:00",
            entries=[
                SnapshotEntry(
                    type="extractors",
                    name="tap-mock",
                    kind="locked",
                    variant="meltano",
                    file="plugins/snapshots/abc/extractors/tap-mock--meltano.lock",
                    is_default_variant=True,
                ),
                SnapshotEntry(
                    type="extractors",
                    name="tap-mock-inherited",
                    kind="inherited",
                    inherit_from="tap-mock",
                ),
            ],
        )
        parsed = LockSnapshotManifest.parse(manifest.canonical())
        assert parsed == manifest
        assert (
            parsed.find_entry(
                plugin_type="extractors",
                plugin_name="tap-mock",
                variant_name="meltano",
            ).name
            == "tap-mock"
        )
        assert (
            parsed.find_entry(
                plugin_type="extractors",
                plugin_name="tap-mock",
                variant_name=None,
            )
            is not None
        )
        assert (
            parsed.find_entry(
                plugin_type="loaders",
                plugin_name="tap-mock",
                variant_name=None,
            )
            is None
        )

    def test_staging(self) -> None:
        staging = Staging(
            snapshot_id="abc",
            state="resolving",
            pid=1234,
            created_at="2026-09-25T00:00:00+00:00",
            base_snapshot_id=None,
            base_project_version="0" * 64,
            hub_origin="https://hub.meltano.com/meltano/api/v1",
            items={
                "extractors/tap-mock--meltano": StagingItem(
                    key="extractors/tap-mock--meltano",
                    type="extractors",
                    name="tap-mock",
                    variant="meltano",
                    fingerprint="a" * 64,
                    status="success",
                    output="plugins/extractors/tap-mock--meltano.lock",
                ),
            },
        )
        data = staging.canonical()
        assert json.loads(json.dumps(data)) == data
        parsed = Staging.parse(data)
        assert parsed == staging
        assert parsed.items["extractors/tap-mock--meltano"].status == "success"


@pytest.mark.usefixtures("tap", "inherited_tap")
class TestStagingAndResolution:
    def test_classify(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
        inherited_tap: ProjectPlugin,
    ) -> None:
        assert subject.classify(tap) == ENTRY_LOCKED
        assert subject.classify(inherited_tap) == ENTRY_INHERITED
        assert subject.item_key(tap) == "extractors/tap-mock--meltano"

    def test_resolve_success(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        staging = subject.begin_batch(subject.read_manifest())
        subject.add_item(staging, tap)
        results = subject.prepare(staging)

        assert len(results) == 1
        result = results[0]
        assert result.status == RESULT_LOCKED
        assert result.variant == "meltano"

        item = staging.items["extractors/tap-mock--meltano"]
        assert item.status == ITEM_SUCCESS
        content_path = subject.staging_dir(staging.snapshot_id) / item.output
        standalone = StandalonePlugin.parse(json.loads(content_path.read_text()))
        assert standalone.name == "tap-mock"
        assert standalone.variant == "meltano"

        assert item.provenance.kind == PROVENANCE_HUB
        assert item.provenance.origin == subject.hub_origin
        assert item.provenance.ref.endswith(
            "/plugins/extractors/tap-mock--meltano",
        )

    def test_resolve_failure(self, subject: LockSnapshotService) -> None:
        bad_plugin = ProjectPlugin(
            PluginType.EXTRACTORS,
            "this-returns-500",
            variant="original",
        )
        staging = subject.begin_batch(subject.read_manifest())
        subject.add_item(staging, bad_plugin)
        results = subject.prepare(staging)

        result = results[0]
        assert result.status == RESULT_FAILED
        assert "500" in result.error

        item = staging.items["extractors/this-returns-500--original"]
        assert item.status == ITEM_FAILED
        assert item.output is None

    def test_retry_reuses_success_and_reruns_failed(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
        hub_request_counter,
    ) -> None:
        # First batch: tap succeeds, bad plugin fails.
        bad_plugin = ProjectPlugin(
            PluginType.EXTRACTORS,
            "this-returns-500",
            variant="original",
        )
        first = subject.begin_batch(subject.read_manifest())
        subject.add_item(first, tap)
        subject.add_item(first, bad_plugin)
        subject.prepare(first)

        # Second batch (retry)
        hub_request_counter.clear()
        second = subject.begin_batch(subject.read_manifest())
        subject.add_item(second, tap)
        subject.add_item(second, bad_plugin)

        # The 500 endpoint is not counted by MockAdapter, so spy on the
        # Hub service to verify the failed item is re-resolved.
        requested: list[str] = []
        original = subject.project.hub_service.find_definition

        def spy(*args: t.Any, **kwargs: t.Any) -> t.Any:
            requested.append(kwargs.get("variant_name"))
            return original(*args, **kwargs)

        subject.project.hub_service.find_definition = spy
        try:
            results = subject.prepare(second)
        finally:
            subject.project.hub_service.find_definition = original

        tap_result = next(r for r in results if r.key == "extractors/tap-mock--meltano")
        bad_result = next(
            r for r in results if r.key == "extractors/this-returns-500--original"
        )
        assert tap_result.status == RESULT_REUSED
        assert bad_result.status == RESULT_FAILED
        # Only the failed item was re-resolved; the reused item made no request.
        assert requested == ["original"]
        assert hub_request_counter["/extractors/tap-mock--meltano"] == 0

    def test_changed_origin_blocks_reuse(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
        monkeypatch: pytest.MonkeyPatch,
        hub_request_counter,
    ) -> None:
        # Seed a successful staging under the current origin.
        first = subject.begin_batch(subject.read_manifest())
        subject.add_item(first, tap)
        subject.prepare(first)

        # Point the project at a different Hub origin.
        monkeypatch.setattr(
            MeltanoHubService,
            "hub_api_url",
            property(
                lambda _self: "https://other-hub.example.com/meltano/api/v1",
            ),
        )

        hub_request_counter.clear()
        second = subject.begin_batch(subject.read_manifest())
        subject.add_item(second, tap)
        results = subject.prepare(second)

        result = results[0]
        # Not reused: fingerprints embed the origin, and the other origin has
        # no adapter, so resolution fails rather than mixing origins.
        assert result.status == RESULT_FAILED
        assert hub_request_counter["/extractors/tap-mock--meltano"] == 0


def _run_batch(
    subject: LockSnapshotService,
    plugins: list[ProjectPlugin],
    *,
    update: bool = False,
) -> tuple[LockSnapshotManifest, Staging]:
    """Run the full batch lifecycle for tests."""
    subject.ensure_can_publish(update=update)
    staging = subject.begin_batch(subject.read_manifest())
    for plugin in plugins:
        subject.add_item(staging, plugin)
    subject.prepare(staging)
    subject.assemble(staging)
    return subject.publish(staging), staging


@pytest.mark.usefixtures("tap", "target")
class TestAssemblyAndPublish:
    def test_publish_first_snapshot(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
        target: ProjectPlugin,
    ) -> None:
        manifest, staging = _run_batch(subject, [tap, target])

        assert subject.pointer_path.exists()
        assert subject.read_manifest().snapshot_id == manifest.snapshot_id

        for entry in manifest.entries:
            path = project.root / entry.file
            assert path.exists()
            StandalonePlugin.parse(json.loads(path.read_text()))

        # The staging is discarded after successful publication.
        assert not subject.staging_dir(staging.snapshot_id).exists()

    def test_publish_carries_unchanged(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
        target: ProjectPlugin,
        hub_endpoints: dict[str, dict],
    ) -> None:
        first, _ = _run_batch(subject, [tap, target])
        base_target_entry = first.find_entry(
            plugin_type="loaders",
            plugin_name="target-mock",
            variant_name=None,
        )
        base_target_bytes = (
            project.root / base_target_entry.file
        ).read_bytes()

        # The Hub definition of tap changes; target is untouched.
        hub_endpoints["/extractors/tap-mock--meltano"]["settings"].append(
            {"name": "foo", "value": "bar"},
        )

        second, _ = _run_batch(subject, [tap], update=True)

        assert len(second.entries) == 2
        target_entry = second.find_entry(
            plugin_type="loaders",
            plugin_name="target-mock",
            variant_name=None,
        )
        assert (project.root / target_entry.file).read_bytes() == base_target_bytes

        tap_entry = second.find_entry(
            plugin_type="extractors",
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        tap_content = json.loads((project.root / tap_entry.file).read_text())
        assert tap_content["settings"][-1]["name"] == "foo"

    def test_legacy_loose_locks_adopted(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        # Batch targets only tap; the target's loose lock exists but has no
        # pointer yet, so it is adopted as a legacy local entry.
        manifest, _ = _run_batch(subject, [tap])

        target_entry = next(
            entry
            for entry in manifest.entries
            if entry.type == "loaders" and entry.name == "target-mock"
        )
        assert target_entry.kind == ENTRY_LEGACY
        assert target_entry.provenance.kind == PROVENANCE_LOCAL

        tap_entry = manifest.find_entry(
            plugin_type="extractors",
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        assert tap_entry.kind == ENTRY_LOCKED
        assert tap_entry.provenance.kind == PROVENANCE_HUB

    def test_assemble_with_failed_item(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        bad_plugin = ProjectPlugin(
            PluginType.EXTRACTORS,
            "this-returns-500",
            variant="original",
        )
        subject.ensure_can_publish(update=False)
        staging = subject.begin_batch(subject.read_manifest())
        subject.add_item(staging, tap)
        subject.add_item(staging, bad_plugin)
        subject.prepare(staging)

        with pytest.raises(SnapshotError):
            subject.assemble(staging)

        assert subject.read_manifest() is None

    def test_origin_mismatch_offline_fails(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first, _ = _run_batch(subject, [tap])

        monkeypatch.setattr(
            MeltanoHubService,
            "hub_api_url",
            property(
                lambda _self: "https://other-hub.example.com/meltano/api/v1",
            ),
        )

        subject.ensure_can_publish(update=True)
        staging = subject.begin_batch(subject.read_manifest())

        with pytest.raises(SnapshotError):
            subject.assemble(staging)

        # The old snapshot is retained; no new pointer was published.
        assert subject.read_manifest().snapshot_id == first.snapshot_id

    def test_ensure_can_publish(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        assert subject.ensure_can_publish(update=False) is None
        _run_batch(subject, [tap])

        with pytest.raises(SnapshotExistsError):
            subject.ensure_can_publish(update=False)
        assert subject.ensure_can_publish(update=True) is None

    def test_staged_snapshot_invisible_until_publish(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        subject.ensure_can_publish(update=False)
        staging = subject.begin_batch(subject.read_manifest())
        subject.add_item(staging, tap)
        subject.prepare(staging)
        manifest = subject.assemble(staging)

        # The snapshot dir and its manifest exist, but the pointer has not
        # switched: no reader can observe the new snapshot yet.
        assert not subject.pointer_path.exists()
        assert (
            subject.snapshot_dir(staging.snapshot_id) / "snapshot.json"
        ).exists()

        subject.publish(staging)
        pointer_data = json.loads(subject.pointer_path.read_text())
        assert pointer_data["snapshot_id"] == manifest.snapshot_id


def _prepare_assemble(subject: LockSnapshotService, plugin: ProjectPlugin) -> Staging:
    staging = subject.begin_batch(subject.read_manifest())
    subject.add_item(staging, plugin)
    subject.prepare(staging)
    subject.assemble(staging)
    return staging


@pytest.mark.usefixtures("tap", "target")
class TestConcurrentBatches:
    def test_second_publish_conflicts(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
        target: ProjectPlugin,
    ) -> None:
        # Both batches are based on the absence of a pointer.
        first = _prepare_assemble(subject, tap)
        second = _prepare_assemble(subject, target)

        subject.publish(first)

        with pytest.raises(SnapshotConflictError) as exc_info:
            subject.publish(second)

        assert exc_info.value.project_version_changed is False
        assert subject.read_manifest().snapshot_id == first.snapshot_id
        # The losing batch keeps its staging, ready for a re-run.
        assert subject.staging_dir(second.snapshot_id).exists()

    def test_project_version_change_conflicts(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
    ) -> None:
        staging = _prepare_assemble(subject, tap)

        # The project changes while the batch was being prepared.
        project.meltanofile.write_text(
            project.meltanofile.read_text() + "\n",
        )

        with pytest.raises(SnapshotConflictError) as exc_info:
            subject.publish(staging)

        assert exc_info.value.project_version_changed is True
        assert subject.read_manifest() is None

    def test_publish_lock_serializes(self, subject: LockSnapshotService) -> None:
        # POSIX fcntl locks are owned by the process, so a separate process
        # is required to verify that publication is serialized.
        path = subject.batch_lock_path
        path.parent.mkdir(parents=True, exist_ok=True)

        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import fasteners, time; "
                    f"lock = fasteners.InterProcessLock(r'{path}'); "
                    "lock.acquire(); time.sleep(2); lock.release()"
                ),
            ],
        )
        try:
            time.sleep(0.5)
            other = fasteners.InterProcessLock(path)
            assert other.acquire(timeout=1) is False
        finally:
            holder.wait(timeout=10)


def _is_dead(pid: int) -> bool:
    """Whether no process exists with the given PID."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _dead_pid() -> int:
    """Find a PID that no process is using."""
    try:
        return next(
            pid
            for pid in range(500_000, 600_000)
            if _is_dead(pid)
        )
    except StopIteration as exc:
        errmsg = "Could not find an unused PID"
        raise RuntimeError(errmsg) from exc


def _write_raw_staging(
    subject: LockSnapshotService,
    *,
    snapshot_id: str,
    state: str,
    pid: int,
    base_snapshot_id: str | None = None,
    base_project_version: str | None = None,
) -> Staging:
    """Write a staging index directly, for recovery tests."""
    staging = Staging(
        snapshot_id=snapshot_id,
        state=state,
        pid=pid,
        created_at=subject._now(),
        base_snapshot_id=base_snapshot_id,
        base_project_version=(
            base_project_version or subject.project_version()
        ),
        hub_origin=subject.hub_origin,
    )
    subject._write_staging(staging)
    return staging


@pytest.mark.usefixtures("tap", "target")
class TestRecovery:
    def test_recover_completes_staged(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
    ) -> None:
        staging = subject.begin_batch(None)
        subject.add_item(staging, tap)
        subject.prepare(staging)
        subject.assemble(staging)

        # The owning process dies after assembling but before publishing.
        staging.pid = _dead_pid()
        subject._write_staging(staging)

        subject.recover()

        manifest = subject.read_manifest()
        assert manifest.snapshot_id == staging.snapshot_id
        for entry in manifest.entries:
            assert (project.root / entry.file).exists()
        assert not subject.staging_dir(staging.snapshot_id).exists()

    def test_recover_completes_after_pointer_switch(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        manifest, _ = _run_batch(subject, [tap])

        # The pointer was switched; only the staging cleanup was interrupted.
        _write_raw_staging(
            subject,
            snapshot_id=manifest.snapshot_id,
            state="publishing",
            pid=_dead_pid(),
            base_snapshot_id=None,
        )

        subject.recover()
        assert not subject.staging_dir(manifest.snapshot_id).exists()
        assert subject.read_manifest().snapshot_id == manifest.snapshot_id

    def test_recover_discards_resolving(
        self,
        subject: LockSnapshotService,
    ) -> None:
        snapshot_id = "a" * 32
        _write_raw_staging(
            subject,
            snapshot_id=snapshot_id,
            state=STATE_RESOLVING,
            pid=_dead_pid(),
        )

        subject.recover()

        assert not subject.staging_dir(snapshot_id).exists()
        assert subject.read_manifest() is None

    def test_recover_discards_stale_base(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        manifest, _ = _run_batch(subject, [tap])

        # Staged against a pointer that no longer exists.
        _write_raw_staging(
            subject,
            snapshot_id="b" * 32,
            state=STATE_STAGED,
            pid=_dead_pid(),
            base_snapshot_id="z" * 32,
        )
        # Staged against the current pointer but from a changed project.
        _write_raw_staging(
            subject,
            snapshot_id="c" * 32,
            state=STATE_STAGED,
            pid=_dead_pid(),
            base_snapshot_id=manifest.snapshot_id,
            base_project_version="0" * 64,
        )

        subject.recover()

        assert not subject.staging_dir("b" * 32).exists()
        assert not subject.staging_dir("c" * 32).exists()
        assert subject.read_manifest().snapshot_id == manifest.snapshot_id

    def test_recover_leaves_live_staging(
        self,
        subject: LockSnapshotService,
    ) -> None:
        snapshot_id = "d" * 32
        _write_raw_staging(
            subject,
            snapshot_id=snapshot_id,
            state=STATE_RESOLVING,
            pid=os.getpid(),
        )

        subject.recover()

        assert subject.staging_dir(snapshot_id).exists()
        shutil.rmtree(subject.staging_dir(snapshot_id))

    def test_recover_prunes_unreferenced_snapshots(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        manifest, _ = _run_batch(subject, [tap])

        ghost = subject.snapshot_dir("ghost")
        ghost.mkdir(parents=True)
        (ghost / "snapshot.json").write_text("{}")

        # A live staging must not be mistaken for stale state.
        _write_raw_staging(
            subject,
            snapshot_id="e" * 32,
            state=STATE_RESOLVING,
            pid=os.getpid(),
        )

        subject.recover()

        assert not ghost.exists()
        assert subject.snapshot_dir(manifest.snapshot_id).exists()
        shutil.rmtree(subject.staging_dir("e" * 32))


@pytest.mark.usefixtures("tap", "target")
class TestSnapshotReadPath:
    def test_load_content_prefers_snapshot(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
        target: ProjectPlugin,
    ) -> None:
        manifest, _ = _run_batch(subject, [tap, target])
        lock_service = PluginLockService(project)

        content, metadata = lock_service.load_content(
            plugin_type=PluginType.EXTRACTORS,
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        assert content["variant"] == "meltano"
        assert metadata.is_default is not None

        # Tamper the loose lock; the snapshot read must not reflect it.
        loose_path = lock_service.lock_path(
            plugin_type=PluginType.EXTRACTORS,
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        loose_path.write_text('{"tampered": true}')

        content2, _ = lock_service.load_content(
            plugin_type=PluginType.EXTRACTORS,
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        assert "tampered" not in content2
        assert content2["variant"] == "meltano"

        definition = lock_service.load_definition(
            plugin_type=PluginType.EXTRACTORS,
            plugin_name="tap-mock",
            variant_name="meltano",
        )
        assert definition.name == manifest.snapshot_id or definition.name == "tap-mock"

    def test_get_standalone_data_prefers_snapshot(
        self,
        subject: LockSnapshotService,
        tap: ProjectPlugin,
    ) -> None:
        _run_batch(subject, [tap])

        standalone = PluginLockService(subject.project).get_standalone_data(tap)
        assert standalone["name"] == "tap-mock"

        # Tamper the loose lock; the snapshot must still be used.
        loose_path = PluginLockService(subject.project).plugin_lock_path(
            plugin=tap,
            variant_name=tap.variant,
        )
        loose_path.write_text('{"tampered": true}')

        standalone2 = PluginLockService(subject.project).get_standalone_data(tap)
        assert "tampered" not in standalone2

    def test_unlisted_plugin_falls_back(
        self,
        subject: LockSnapshotService,
        project: Project,
        tap: ProjectPlugin,
    ) -> None:
        # The snapshot covers only tap; target is not listed.
        _run_batch(subject, [tap])

        # Provide a loose lock for a plugin that is neither in the project
        # nor listed in the snapshot.
        fallback_path = project.dirs.plugin_lock_path(
            PluginType.LOADERS,
            "target-unlisted",
        )
        fallback_path.parent.mkdir(parents=True, exist_ok=True)
        fallback_path.write_text('{"fallback": true}')

        content, _ = PluginLockService(project).load_content(
            plugin_type=PluginType.LOADERS,
            plugin_name="target-unlisted",
        )
        assert content == {"fallback": True}
