# Meltano 插件安装事务化 - Implementation Plan

实现以新包 `src/meltano/core/install_transaction/` 承载，职责拆分为 paths / plan / staging / commit / recovery / orchestration；测试镜像至 `tests/meltano/core/install_transaction/`。每任务包含随片测试。

## Task 1: 事务路径布局（install_transaction.paths）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-1.1/1.2/1.3 pass: `tests/meltano/core/install_transaction/test_paths.py` 5/5 通过；venv 路径 == `project.dirs.venvs(...)`，默认无触盘，ensure_layout 幂等。
  - 新增 [paths.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/paths.py) 与包 `__init__.py`。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新建包 `src/meltano/core/install_transaction/__init__.py` 与 `paths.py`，实现 `InstallPaths`：基于 `project.dirs` 给出插件事务根 `.meltano/<type>/<plugin_dir_name>/` 下的：
    - `venv`（最终 venv，路径与现状一致）
    - `state_path` → `install.json`
    - `staging_root` → `staging/`；`staging_dir(plan_id)` → `staging/<plan_id>/`，其下 `venv`、`plan_file`（plan.json）、`candidate_state`（install.json）、`markers`（阶段标记）
    - `recovery_root` → `recovery/`；`recovery_record_path(plan_id)`
    - `locks_dir` → `locks/`；`commit_lock_path`
    - 切换备份名 `venv_backup`（含旧 plan_id）
  - 所有路径默认 `make_dirs=False`（dry-run 友好），需要时由调用方显式创建；提供 `ensure_layout()` 仅创建非 dry-run 所需目录。
  - 在 [project_dirs_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/project_dirs_service.py) 不新增破坏性方法，路径逻辑全部收敛在新包。
- **Acceptance Criteria Addressed**: AC-2, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-1.1: 给定项目与插件，`InstallPaths` 各路径字符串与现有约定一致（venv == `project.dirs.venvs(type, dir_name)`），且默认不创建任何目录；证据：路径断言 + `exists()` 全 False
  - `rule` TR-1.2: `ensure_layout()` 后 staging/recovery/locks 父目录存在；证据：临时项目中目录存在性断言
  - `rule` TR-1.3: 两个不同插件生成的锁/暂存根路径互不相同；证据：对比断言

## Task 2: 安装计划模型与解析服务（plan.py）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1~2.4 pass: `tests/meltano/core/install_transaction/test_plan.py` 18/18 通过；plan_id 确定性与 pip_url/variant/python/lock 变更敏感性、offline 无 Hub 调用且缺 lock 抛 InstallResolutionError、JSON 往返、MELTANO_OFFLINE 真值均验证。
  - 新增 [plan.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/plan.py) 与 [errors.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/errors.py)。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 实现 frozen dataclass `InstallPlan`：plan_id、plugin 身份（type、name、plugin_dir_name、variant、namespace）、python、pip_install_args、lock_content（dict 快照）、lock_hash、reason、created_at、meltano 版本；提供 `canonical()`/序列化与 `fingerprint`（复用 `venv_service.fingerprint`）。
  - 实现 `InstallPlanService`：
    - `resolve(plugin, reason, *, offline=False) -> InstallPlan`：pip args 复用 `get_pip_install_args`（传入 `plugin_installation_env` 等价环境，保留 env var 扩展与 AUTO 缺变量跳过语义）；lock 快照读取 `PluginLockService` 对应路径：在线且文件缺失时沿用现有「从 Hub 获取」行为；`offline=True` 时严禁 Hub 访问，lock 缺失抛 `InstallResolutionError(MeltanoError)`（可操作 instruction）。
    - plan_id = sha256(canonical JSON of {plugin_type, plugin_dir_name, variant, sorted(set(pip_install_args)), python, lock_hash})，键序固定。
    - 提供 `truthy_offline`（解析 `MELTANO_OFFLINE`：1/true/yes/on）。
  - 新增错误类（`InstallTransactionError(MeltanoError)` 及 `InstallResolutionError`、`StalePlanError`、`StagingBuildError`、`ExecutabilityCheckError`、`OfflineUnavailableError`），集中在包内 errors 模块，全部带 instruction。
- **Acceptance Criteria Addressed**: AC-1, AC-11, AC-14
- **Test Requirements**:
  - `rule` TR-2.1: 相同输入两次解析 plan_id 相等；分别改动 pip_url、variant、lock 文件内容、python 后 plan_id 均改变；证据：参数化测试
  - `rule` TR-2.2: offline 解析不触发任何 Hub 调用（mock Hub 并断言零调用），lock 缺失时抛 `InstallResolutionError` 且含 instruction；证据：异常与 mock 断言
  - `rule` TR-2.3: `InstallPlan` 可 JSON 往返且字段无损；证据：序列化/反序列化相等断言
  - `rule` TR-2.4: `MELTANO_OFFLINE` 真值解析符合 1/true/yes/on 约定，其余为 False；证据：参数化测试

## Task 3: 暂存构建器与可执行性检查（staging.py）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1~3.4 pass: `tests/meltano/core/install_transaction/test_staging.py` 9/9 通过；构建目标为暂存路径且最终 venv 不变；离线参数 uv `--offline` / pip `--no-index`(+find-links) 正确；真实临时 venv 中可执行性检查四参数化场景符合预期；deps marker 与指纹写入验证。
  - 扩展 [venv_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/venv_service.py) 支持 `venv_path` 覆盖；新增 [staging.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/staging.py)。
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Description**:
  - 扩展 [venv_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/venv_service.py)：`VenvBackend.from_plugin` 与 `VirtualEnvService.from_plugin` 增加可选 `venv_path: Path | None = None`（默认沿用 `project.dirs.venvs(...)`），使同一后端可构建到暂存根；log_path 保留在原 logs 目录。
  - 实现 `StagingBuilder`：
    - `prepare(plan)`：创建 `staging/<plan_id>/`；写 plan.json 与阶段 marker（`venv-created`/`deps-installed`/`checked`），使重启可判断进度。
    - `build(plan)`：在暂存 venv 执行 create + install；离线参数注入——uv 后端 args 前置 `--offline`；pip 后端 args 前置 `--no-index`（存在 `MELTANO_PIP_FIND_LINKS` 时追加 `--find-links`）；构建失败抛 `StagingBuildError`/`OfflineUnavailableError`。
    - `is_complete(plan)`：marker + 指纹判断，供复用。
  - 实现 `ExecutabilityChecker`：
    1. 暂存 `bin/python` 存在并执行 `-c "import sys"`（超时）；
    2. `venv.exec_path(plugin.executable)` 存在且 `os.access(X_OK)`；
    3. 以可执行文件运行 `--help`（超时，stdout/stderr 合并读取）；退出 0 通过，否则抛 `ExecutabilityCheckError`（含输出尾部）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-11
- **Test Requirements**:
  - `rule` TR-3.1: build 调用的后端目标路径 == 暂存 venv；最终 venv 在 build 前后无变化（快照对比）；证据：mock 后端参数 + 路径快照
  - `rule` TR-3.2: 离线 build 时 uv 调用含 `--offline`、pip 调用含 `--no-index`；证据：mock 调用参数断言
  - `rule` TR-3.3: 在真实临时 venv 中：正常脚本通过检查；缺可执行文件、`--help` 退出非 0、python 缺失三种构造分别抛对应错误；证据：checker 参数化测试
  - `rule` TR-3.4: build 成功后写出 `deps-installed` marker 且暂存指纹 == 计划指纹；证据：marker 与指纹断言

## Task 4: 原子提交器（commit.py）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1~4.4 pass: `tests/meltano/core/install_transaction/test_commit.py` 4/4 通过；提交后 venv/install.json 与计划匹配且暂存/备份清除；stale plan 拒绝且不触盘；状态写入失败回滚旧版本并还原暂存；跨进程持锁时等待超时给出可操作错误。
  - 新增 [commit.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/commit.py)。
- **Priority**: high
- **Depends On**: Task 2, Task 3
- **Description**:
  - 实现 `InstallCommitter`：
    - 以 `fasteners.InterProcessLock(commit_lock_path)` 作为 per-plugin 提交锁（blocking 获取，带默认超时，超时抛可操作错误）。
    - `commit(plan)`：锁内先 `plan_service.resolve(...)` 复检——当前 plan_id 与暂存计划不一致即抛 `StalePlanError`（不切换）。
    - 切换序列（同卷 rename）：旧 venv → `venv.backup-<old_id>`（无旧版本则跳过）→ 暂存 venv → venv → 校验新 python 存在/指纹 → 候选 install.json（临时文件 + `os.replace` 原子落位）→ 校验通过后删除 backup；任何一步异常：存在 backup 则回滚 rename 恢复旧 venv、删除半成品，抛出并由调用方记录恢复。
    - install.json 内容：plan_id、插件身份、python、pip args、lock_hash、fingerprint、committed_at、meltano 版本。
  - 提供 `CommittedState` 读取模型（`load(paths)`，损坏/缺失返回 None）。
- **Acceptance Criteria Addressed**: AC-4, AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-4.1: 提交后 venv 内容来自暂存、python 可运行、`install.json` 字段与计划完全匹配、暂存目录被清空；证据：提交器测试
  - `rule` TR-4.2: 复检发现当前 plan_id != 暂存 plan_id 时抛 `StalePlanError`，venv/install.json 均不变；证据：stale-plan 测试
  - `rule` TR-4.3: 在「venv 就位后、状态写入前」注入失败：backup 被回滚，最终 venv 与 install.json 仍为旧版本；证据：切换中途失败测试
  - `rule` TR-4.4: 同一 commit_lock_path 上两个获取者串行（线程模拟），第二个获取时重新复检；证据：锁行为测试

## Task 5: 恢复协调器（recovery.py）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1~5.3 pass: `tests/meltano/core/install_transaction/test_recovery.py` 8/8 通过；失败记录字段完整、重名不覆盖；reconcile 覆盖 reusable/resumable/过期清理/损坏清理/切换回滚/备份清除；supersede 保留文件。
  - 新增 [recovery.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/recovery.py)。
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 4
- **Description**:
  - 实现 `RecoveryRecord`（dataclass）：record_id、plan_id、插件身份、失败阶段、原因、时间戳、staging 路径、previous_plan_id、状态（open/superseded）、建议；原子写入（tmp + os.replace）。
  - 实现 `RecoveryService`：
    - `record_failure(plan, stage, error, previous_plan_id)`：写 `recovery/<plan_id>-<timestamp>.json`（文件名可识别、不覆盖既有记录）。
    - `list_open()` / `supersede_for(plugin)`：新成功提交后将同插件 open 记录标记 superseded（不删除）。
    - `reconcile(plugin) -> ReconcileResult`：
      1. 发现 `venv.backup-*` 且 venv 缺失/损坏 → 回滚恢复 backup；venv 完好而 backup 残留 → 删除 backup；
      2. 扫描 staging/：与当前 plan_id 相同 → 校验 marker/指纹，返回 reusable（完整）或 resumable（不完整）；其他 plan_id 或损坏 → rmtree；
      3. 汇总 open recovery 记录并在日志输出路径。
- **Acceptance Criteria Addressed**: AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-5.1: `record_failure` 写出的 JSON 字段完整可解析，重复失败产生不同文件名（不覆盖）；证据：记录内容与文件数断言
  - `rule` TR-5.2: 完整同计划暂存 → reusable 且无 pip 调用；不完整 → resumable；异计划/损坏 → 目录被删除；四种切换残留情形按规格恢复；证据：reconcile 参数化测试（对应 AC-6 四情形）
  - `rule` TR-5.3: 成功提交后旧 open 记录变为 superseded 但文件仍存在；证据：状态字段断言

## Task 6: 事务编排与 PluginInstallService 接入（transaction.py + service 改造）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1~6.5 pass: `test_transaction.py` 8 项 + 既有 `test_plugin_install_service.py` 20 项通过；快速路径零 pip 调用、dry-run 目录树无变化、三阶段失败注入保留旧版本且 recovery 完整、首次安装失败可重试、并发单提交者/异插件并行均验证。连续 5 次随机顺序 52/52 稳定。
  - 新增 [transaction.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/install_transaction/transaction.py)；改造 [plugin_install_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/plugin_install_service.py)。
- **Priority**: high
- **Depends On**: Task 2, Task 3, Task 4, Task 5
- **Description**:
  - 实现 `InstallTransaction` 编排单插件事务：`recovery.reconcile` → fast-path 判定（CommittedState.plan_id == 当前 plan_id 且 venv python 存在、指纹匹配 → SKIPPED）→ 按 reusable/resumable 复用或 `staging.build` → `ExecutabilityChecker` → `committer.commit` → hooks → `recovery.supersede_for`；各阶段异常统一：保留旧版本（首次安装无旧版本时保留无 venv 状态）、`record_failure`、返回 ERROR 状态与可操作消息。
  - 改造 [plugin_install_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/plugin_install_service.py)：
    - `__init__` 增加 `dry_run: bool = False`、`offline: bool = False`（offline 可由 MELTANO_OFFLINE 初始化）。
    - `install_plugin_async`：保留全局 semaphore 与 `remove_duplicates`；dry-run 仅 resolve + 产出 RUNNING/SKIPPED 状态后返回，全程不触盘；非 dry-run 走 `InstallTransaction`。
    - hooks：成功提交后触发 `trigger_hooks("install", ...)`（语义不变，hook 失败 → WARNING）。
    - `_requires_install` 逻辑由 fast-path/计划复用取代（AUTO 缺 env var 跳过语义保留在 resolve 阶段）。
  - 模块级 `install_plugins(...)` helper 增加 `dry_run`/`offline` kwarg（默认 False，向后兼容），保持返回 bool。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-8, AC-9, AC-10, AC-12, AC-13, AC-14, AC-15
- **Test Requirements**:
  - `rule` TR-6.1: fast-path 条件满足时无暂存目录、无 pip/uv 调用、状态 SKIPPED；证据：mock 零调用 + 状态断言
  - `rule` TR-6.2: dry-run 全流程无文件/目录产生（项目前后文件树 diff 为空）、无子进程，状态含将执行动作；证据：快照对比
  - `rule` TR-6.3: 端到端三阶段失败注入后旧版本可运行且 recovery 记录存在（AC-5 编排层复验）；证据：参数化集成测试
  - `rule` TR-6.4: 首次安装（无旧版本）失败后不存在 venv 与 install.json，且再次运行可成功；证据：首次安装失败/重试测试
  - `rule` TR-6.5: `install_plugins` helper 新参数默认值不破坏既有位置参数调用方式；证据：签名/调用测试

## Task 7: CLI 选项（install.py / upgrade.py 接线）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1/7.2 pass: `tests/meltano/cli/test_install.py` 13/13 通过；--dry-run/--offline 与 MELTANO_OFFLINE 均正确透传，既有 kwargs 断言已更新。
  - 改造 [cli/install.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/cli/install.py)。
- **Priority**: high
- **Depends On**: Task 6
- **Description**:
  - [cli/install.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/cli/install.py) 增加：
    - `--dry-run`：解析并报告安装计划但不写磁盘。
    - `--offline`：只使用已锁定且本地可用制品；未显式传入时读 `MELTANO_OFFLINE`。
    - 两选项透传至 `install_plugins(...)`；help 文案说明语义与恢复记录位置。
  - 其余调用方（add/config/elt/invoke/run/validate/upgrade）不强制改动；其 `install_plugins(...)` 调用因默认值保持兼容。
- **Acceptance Criteria Addressed**: AC-9, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-7.1: `meltano install --dry-run` 调用 helper 时 `dry_run=True`；`--offline` 时 `offline=True`；设置 `MELTANO_OFFLINE=true` 且不传选项时 `offline=True`；证据：CLI mock 调用断言
  - `rule` TR-7.2: 既有 CLI 用例（不带新选项）调用参数仅新增 `dry_run=False, offline=False`；更新 [test_install.py](file:///Users/ding/Documents/swe/09224/project-07/tests/meltano/cli/test_install.py) 中精确 kwargs 断言；证据：更新后测试通过

## Task 8: 全量验证与既有测试回归
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 6, Task 7
- **Description**:
  - 运行新包全部测试与受影响测试：`tests/meltano/core/install_transaction/`、`test_plugin_install_service.py`、`cli/test_install.py`，以及调用安装的 cli/core 测试目录（add/config/elt/invoke/run/validate/upgrade 相关）。
  - 运行 `ruff`（含 import 排序）、`mypy`/`ty` 类型检查；修正所有诊断。
  - 核对 AC-12/AC-13/AC-14 三个 rubric 的自评与证据，补齐遗漏边界。
- **Acceptance Criteria Addressed**: AC-1~AC-14（验证收口）
- **Test Requirements**:
  - `rule` TR-8.1: 受影响测试全部通过（允许标记 slow 的用例按需运行，结果记录）；证据：pytest 输出
  - `rule` TR-8.2: `nox -t lint` 等价的 ruff 检查与 `nox -s typing` 类型检查零错误；证据：命令输出
  - `rubric` TR-8.3: 测试覆盖质量；scale 1-5；anchors 1 = 仅 happy path，3 = 主路径覆盖，5 = 成功/失败/重启/配置变更/并发/离线/dry-run 全覆盖；threshold >= 4；证据：测试清单与评审
