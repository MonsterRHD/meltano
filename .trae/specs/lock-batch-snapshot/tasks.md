# meltano lock 批次快照 - Implementation Plan

## Task 1: 快照路径、数据结构与指纹

- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-1.1 pass: `project_dirs_service.py` 新增 4 个路径方法，测试 `TestSnapshotPaths` 断言指针/快照/暂存/批次锁路径全部正确。
  - TR-1.2 pass: `TestFingerprint` 验证指纹稳定且对 type/name/variant/hub origin/本地定义变化敏感。
  - TR-1.3 pass: `TestRoundTrip` 验证 provenance/entry/manifest/staging JSON 往返无损。
  - 命令证据：`uv run python -m pytest tests/meltano/core/test_lock_snapshot_service.py` → 18 passed。
- **Description**:
  - 在 `ProjectDirsService` 增加快照相关路径：快照指针 `plugins/lock.snapshot.json`、快照根目录 `plugins/snapshots/`、暂存根目录 `.meltano/lock/staging/`、批次进程锁 `.meltano/run/lock-snapshot.lock`。
  - 在新模块 `src/meltano/core/lock_snapshot_service.py` 中定义数据结构：`DefinitionProvenance`（kind/origin/ref）、快照清单（snapshot_id、项目版本、environment、条目列表，条目含 type/name/variant/file/provenance/variant 元数据或 inherit_from/custom/legacy 标记）、暂存索引（pid、state、base_snapshot_id、base_project_version、逐项 fingerprint/status/error）。
  - 实现输入指纹：对相同 (type, root_name, variant, hub origin) 稳定，对任一变化敏感；本地定义含规范化内容。
  - 定义错误类型：`SnapshotConflictError`、`SnapshotExistsError`（或复用既有错误，最终以实现为准）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-10
- **Test Requirements**:
  - `rule` TR-1.1: 各路径方法返回 `<root>/plugins/lock.snapshot.json`、`<root>/plugins/snapshots/<id>`、`<sys>/lock/staging`、`<sys>/run/lock-snapshot.lock`；evidence 为服务层测试断言。
  - `rule` TR-1.2: 指纹对相同输入字节一致；改变 plugin type/root name/variant/hub origin/本地定义任一字段后指纹不同；evidence 为指纹参数化测试。
  - `rule` TR-1.3: 快照清单与暂存索引可序列化为 JSON 并无损反序列化；evidence 为往返（round-trip）测试。

## Task 2: 暂存区、逐项解析与分类

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-2.1 pass: `TestStagingAndResolution::test_classify` 断言根/继承分类与 key 正确。
  - TR-2.2 pass: `test_resolve_success` 验证暂存文件为 StandalonePlugin JSON，索引含 provenance(hub origin/ref)、variant 元数据、fingerprint、status。
  - TR-2.3 pass: `test_resolve_failure`（500 插件）验证 failed 状态、错误信息保留、无输出文件。
  - TR-2.4 pass: `test_retry_reuses_success_and_reruns_failed` 与 `test_changed_origin_blocks_reuse`：指纹一致成功项复用且无 Hub 请求（spy/counter 双验证），origin 改变后不复用。
  - 命令证据：pytest（默认随机序 + `-p no:randomly` 两种顺序）→ 23 passed。
- **Description**:
  - 实现 `LockSnapshotService` 的目标分类：根插件（可锁定）、继承插件（记录 inherit_from）、自定义插件（custom 标记）。
  - 实现项目版本计算（`meltano.yml` 内容 SHA-256）。
  - 创建新批次暂存（snapshot_id、基线指针、基线项目版本、pid、state=resolving），逐项解析：Hub 路径经 `hub_service.find_definition` 得到定义、variant 元数据与 provenance(origin=hub_api_url, ref=定义端点)；解析结果以独立锁文件写入暂存目录；异常被捕获并逐项记录。
  - 实现暂存索引读写与“从最近历史暂存按指纹播种复用”：只复用成功且指纹一致项。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-5, AC-6, AC-10
- **Test Requirements**:
  - `rule` TR-2.1: 给定含根/继承/自定义插件的插件列表，分类结果逐项正确；evidence 为分类测试。
  - `rule` TR-2.2: 成功项暂存文件内容为 StandalonePlugin canonical JSON，索引含 provenance、variant 元数据、fingerprint、status；evidence 为读取暂存文件与索引的测试。
  - `rule` TR-2.3: 对解析失败项，状态为 failed 并记录错误信息，其他成功项不受影响；evidence 为 Hub 返回 500 场景测试。
  - `rule` TR-2.4: 第二次解析运行对指纹一致成功项不产生 Hub 请求（请求计数为 0），失败项重新请求；origin 改变后该项不复用；evidence 为 MockAdapter 计数测试。

## Task 3: 快照组装、携带、origin 校验与原子发布

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - TR-3.1 pass: `test_publish_first_snapshot` 验证发布后指针存在、清单文件 100% 存在且可解析、暂存被清理。
  - TR-3.2 pass: `test_publish_carries_unchanged`（Hub tap 变更）与 `test_legacy_loose_locks_adopted`：非目标条目字节携带，散列锁以 legacy 本地条目进入首个快照。
  - TR-3.3 pass: `test_origin_mismatch_offline_fails` 与 `test_assemble_with_failed_item`：origin 不一致旧条目触发重新解析，离线失败时不发布且旧快照保留。
  - TR-3.4 pass: `test_staged_snapshot_invisible_until_publish`：staged 阶段快照目录已存在但指针未切换，发布后才可见。
  - 命令证据：pytest 随机/固定两种顺序 → 30 passed。
- **Description**:
  - 全部目标成功后，将暂存项复制进自包含快照目录；从基线快照携带非目标条目（字节复制，含 provenance），无指针时将散列锁文件读取为 legacy 本地条目。
  - 组装时执行 origin 校验：hub 条目 origin 必须等于当前 hub api root；不一致的非目标携带条目不纳入（因此快照不完整 → 批次失败）；legacy/local 条目不参与 hub origin 约束。
  - 写入快照清单文件，state 置 staged；发布时先写指针临时文件再 `os.replace` 到 `plugins/lock.snapshot.json`，随后 state=publishing→清理暂存与无引用快照目录。
  - 已存在指针且未要求更新（CLI `--update`）时拒绝新批次。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-9, AC-10
- **Test Requirements**:
  - `rule` TR-3.1: 发布后指针指向新 snapshot_id，清单中每个 file 存在且 JSON 可解析；evidence 为发布后扫描测试。
  - `rule` TR-3.2: 新快照条目 = 携带条目 ∪ 目标条目，携带条目与旧快照字节一致；散列锁以 legacy 条目进入首个快照；evidence 为字节对比测试。
  - `rule` TR-3.3: origin 不一致的携带条目不进入快照；离线无法重新解析时批次失败且指针不变；evidence 为 origin 变更 + 连接失败测试。
  - `rule` TR-3.4: 指针切换通过临时文件 + replace 完成；模拟 replace 前时刻断言旧指针仍可用；evidence 为发布步骤检查测试。

## Task 4: 进程间发布锁与并发冲突检测

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - TR-4.1 pass: `test_second_publish_conflicts` 与 `test_project_version_change_conflicts`：指针抢先发布/meltano.yml 变更时，第二次发布抛 SnapshotConflictError、不覆盖、暂存保留。
  - TR-4.2 pass: `test_publish_lock_serializes`（子进程持锁）：同锁另一进程获取失败，发布按进程串行。
  - 修复：快照 GC 引用集合纳入活跃暂存自身的 snapshot_id，防止误删并发批次已组装目录。
  - 命令证据：pytest 随机/固定顺序 → 33 passed。
- **Description**:
  - 发布区段以 `fasteners.InterProcessLock`（`.meltano/run/lock-snapshot.lock`）串行化。
  - 进入发布后重新读取当前指针：当前 snapshot_id（或缺失状态）及项目版本与暂存基线不一致时抛 `SnapshotConflictError`，不修改指针，暂存保留。
  - 基线一致的重复/恢复执行可正常完成，不得产生重复条目。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-4.1: 两个基于同一旧指针的暂存连续发布：第一个成功，第二个抛冲突错误、指针保持第一个快照、第二暂存仍存在；evidence 为服务层双暂存测试。
  - `rule` TR-4.2: 持有进程锁期间另一发布调用阻塞等待而非并发写入；evidence 为以线程/加锁原语验证串行化的测试。

## Task 5: 中断恢复与残留清理

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - TR-5.1 pass: `test_recover_completes_staged` / `test_recover_completes_after_pointer_switch`：死进程 staged/publishing 且基线未变 → 完成发布（含指针已切换的仅清理情形）。
  - TR-5.2 pass: `test_recover_discards_resolving` / `test_recover_discards_stale_base`：未完成组装或基线（指针/项目版本）已变 → 暂存删除、指针不变。
  - TR-5.3 pass: `test_recover_leaves_live_staging` 与 `test_recover_prunes_unreferenced_snapshots`：活进程暂存保留；无引用快照清理、指针快照保留。
  - 命令证据：pytest 随机/固定顺序 → 39 passed。
- **Description**:
  - 批次开始时执行 `recover`：扫描暂存目录，依据 pid 存活（`os.kill(pid, 0)`）、state、基线 snapshot_id 与当前项目版本决定动作：死 pid + staged/publishing + 指针与项目版本未变 → 直接完成发布；死 pid + resolving 或基线变化 → 删除暂存；活 pid 暂存跳过。
  - 清理不被当前指针且不被任何保留暂存引用的快照目录。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-5.1: 死 pid + staged 且基线未变 → 恢复后指针指向该 snapshot_id 且文件完整；evidence 为恢复测试。
  - `rule` TR-5.2: 死 pid + resolving（或基线 snapshot_id/项目版本已变）→ 暂存被删除且指针不变；evidence 为清理测试。
  - `rule` TR-5.3: 活 pid（当前测试进程自身 pid）暂存不被清理；无引用快照目录被清理、指针所指快照保留；evidence 为扫描结果测试。

## Task 6: 读取方快照感知

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 3
- **Completion Evidence**:
  - TR-6.1 pass: `test_load_content_prefers_snapshot` / `test_get_standalone_data_prefers_snapshot`：发布后读取快照内容与 variant 元数据；篡改散文件不影响读取。
  - TR-6.2 pass: `test_unlisted_plugin_falls_back`：未列出插件回退散文件路径；无指针项目由既有测试覆盖（单独运行某既有测试失败已验证为 stash 后同样失败的既有问题）。
  - 命令证据：新文件 42 passed；既有 plugin_lock/locked_definition/cli lock 全套（整文件）5+ passed。
- **Description**:
  - 修改 `PluginLockService.load_content`：存在指针时在清单中匹配 (type, name, variant)；variant 为 None 时匹配默认 variant 条目；从快照文件读取内容，variant 元数据取清单条目。
  - 同步修改 `get_standalone_data`：有指针且条目存在时读快照；其余回退原逻辑。
  - 清单未列出或无指针时，完全保持现有散文件 → Hub 拉取路径。
- **Acceptance Criteria Addressed**: AC-3, AC-9
- **Test Requirements**:
  - `rule` TR-6.1: 发布快照后，`load_content`/`load_definition` 返回快照内容与 variant 元数据；散文件即使存在也不被读取（可篡改散文件证明）；evidence 为读取对比测试。
  - `rule` TR-6.2: 无指针项目行为与现状一致（缺锁时回退/Hub 拉取）；清单未列出插件同样回退；evidence 为兼容性测试。

## Task 7: CLI `--batch` 接线、逐项输出与只读检查

- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 3, Task 4, Task 5
- **Completion Evidence**:
  - TR-7.1 pass: `test_batch_publishes_snapshot` 验证退出码 0、逐项 locked、拓扑警告与指针生成。
  - TR-7.2 pass: `test_batch_failure_keeps_previous_snapshot`：退出码 1、可定位失败插件、指针字节不变，测试后恢复 meltano.yml 原状态。
  - TR-7.3 pass: `test_batch_readonly`（ProjectReadonly）、`test_batch_without_update_when_snapshot_exists`（SnapshotExistsError 提示 --update）、`test_batch_update_relocks_after_success`（成功后更新重新解析）；非批次既有 TestLock 测试全部通过（并修复其一个既有用例的 fixture 隔离）。
  - 命令证据：4 个不同随机种子下 57 passed。
- **Description**:
  - 为 `lock` 命令增加 `--batch/--snapshot` 选项（最终命名以实现一致性为准）；批次模式调用 `LockSnapshotService` 完整流程（恢复 → 解析/复用 → 组装 → 发布）。
  - 输出逐项结果（locked/reused/failed/skipped custom/inherited）与汇总；存在失败时以 `CliError`/非零退出；保持 tracking context。
  - 只读项目写入得到 `ProjectReadonly` 对应的 CLI 错误；已存在快照且未带 `--update` 时明确报错并提示 `--update`。
- **Acceptance Criteria Addressed**: AC-1, AC-5, AC-9
- **Test Requirements**:
  - `rule` TR-7.1: CLI 批次成功运行后指针生成/更新，输出含逐项 locked/reused 与汇总，退出码 0；evidence 为 CliRunner 测试。
  - `rule` TR-7.2: 含失败项的批次退出码 1，输出可定位失败插件与错误，指针不变；evidence 为失败 CLI 测试。
  - `rule` TR-7.3: 只读项目批次报只读错误；已存在快照未带 `--update` 报错；非批次单插件既有行为测试全部通过；evidence 为 CLI 测试与既有测试回归。

## Task 8: 全量验证与质量收口

- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 6, Task 7
- **Completion Evidence**:
  - TR-8.1 pass: lock 四文件全套 57 passed；add/install/plugins 回归 91 passed 1 xfailed。
  - TR-8.2 pass: ruff check 全部通过、ruff format 完成；mypy/ty 与改动文件相关错误为 0（仅剩未安装可选 extras 的既有 unresolved import：aiodocker/google/azure）。
  - TR-8.3 rubric 自评 4/5：核心逻辑集中在 LockSnapshotService 服务类、CLI 仅做参数与输出、全部类型注解、structlog + MeltanoError 子类、测试镜像源码；未得 5 分原因：快照格式为首次引入，尚无跨版本兼容代码可验证。
- **Description**:
  - 运行 lock 相关全部测试、核心服务测试与 CLI 测试；运行 ruff/mypy/ty 对新增/修改文件的检查并修复问题。
  - 对照 spec 逐项核验 AC 覆盖证据，补齐遗漏边界测试。
- **Acceptance Criteria Addressed**: AC-1..AC-11
- **Test Requirements**:
  - `rule` TR-8.1: `uv run python -m pytest tests/meltano/core/test_lock_snapshot_service.py tests/meltano/core/test_plugin_lock_service.py tests/meltano/core/test_locked_definition_service.py tests/meltano/cli/test_lock.py` 全部通过；evidence 为命令输出。
  - `rule` TR-8.2: ruff 与 mypy（或 ty）对改动文件无新增错误；evidence 为检查命令输出。
  - `rubric` TR-8.3: 架构符合度；scale 1-5；anchors 1/3/5 同 AC-11；threshold >= 4；evidence 为代码结构与检查输出自评记录。
