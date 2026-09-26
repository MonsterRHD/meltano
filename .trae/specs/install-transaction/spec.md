# Meltano 插件安装事务化 - Product Requirements Document

## Overview
- **Summary**: 将 `meltano install` 的插件安装过程改造为带身份的事务：先根据当前项目配置与 lock 解析出确定性的安装计划，在隔离暂存目录中完成依赖安装与可执行性检查，再以原子切换提交 venv 与已提交状态记录（含 lock/manifest 视图）；任何步骤失败都保留上一个可运行版本并留下可识别的恢复记录。
- **Purpose**: 修复升级插件时中断 `meltano install` 导致「半成品虚拟环境 + 已更新 lock + 旧项目清单」混用的问题，使安装可恢复、可并发、可离线、可预演。
- **Target Users**: 在本地、CI 与生产环境中运行 `meltano install` / `meltano upgrade` 的数据工程师，以及自动触发安装（`run`/`elt`/`invoke` 的 AUTO 安装）的所有用户。

## Goals
- 安装的每一步要么完整提交，要么完全不影响当前可运行版本。
- 进程崩溃/重启后能自动复用同计划暂存物、清理过期暂存物，并可从中断的切换中恢复。
- 同一插件的跨进程并发安装只有一个提交者；不同插件仍可完全并行。
- 支持 `--dry-run`（不写磁盘）、快速路径（无变更时不执行 pip）、离线模式（只用已锁定且本地可用制品）。
- 所有失败都给出可识别的恢复记录与可操作（user-actionable）的错误信息。

## Non-Goals
- 不改变 `meltano compile` 生成 manifest 的方式，不新增独立的 manifest 快照文件（已确认：视图以「已提交状态记录」形式落地）。
- 不改变插件运行时定位 venv 的路径约定（`.meltano/<plugin_type>/<plugin_dir_name>/venv`）。
- 不重构 lock 文件的磁盘格式与 `meltano lock` 的写入逻辑；事务只读取/快照 lock 内容。
- 不引入分布式锁或跨主机并发协调；并发范围限定为同一项目文件系统上的进程。
- File 类型插件（无 venv）的 `after_install` 文件写入不纳入暂存事务，仍在提交后按现有 hook 机制执行。

## Background & Context
- 当前 [plugin_install_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/plugin_install_service.py) 的 `install_pip_plugin` 通过 `VirtualEnvService.from_plugin` 直接在最终位置 `.meltano/<type>/<name>/venv` 原地执行 `uv/pip install`，指纹文件 `.meltano_plugin_fingerprint` 在安装最后才写入；中途中断会留下损坏 venv。
- Lock 位于项目根 `plugins/<type>/<name>[--<variant>].lock`（见 [project_dirs_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/project_dirs_service.py)），由 [plugin_lock_service.py](file:///Users/ding/Documents/swe/09224/project-07/src/meltano/core/plugin_lock_service.py) 读写；venv 与 lock 之间没有任何事务关联或一致性记录。
- 进程内重复插件已由 `PluginInstallService.remove_duplicates` 按 venv 去重；跨进程没有任何互斥。
- 项目已依赖 `fasteners`（进程间锁、读写锁）与 `structlog`；AGENTS.md 要求新核心功能以新服务类承载、完整类型标注、子类化 `MeltanoError` 提供可操作信息、注意 asyncio 开销。
- 已确认的用户决策：
  1. 提交视图 = 已提交状态 JSON（计划身份、venv 指纹、pip args、lock 内容哈希与快照），与 venv 原子切换；
  2. 离线入口 = 新增 `--offline` CLI 选项 + `MELTANO_OFFLINE` 环境变量；
  3. 暂存 venv 在下次运行时「同计划复用、过期清理」；恢复记录持久保留，直到被后续成功安装标记为 superseded 或用户手动删除。

## Functional Requirements
- **FR-1 安装计划解析**：安装开始前，根据当前项目的 `ProjectPlugin`（type、name/plugin_dir_name、variant、pip_url、python）与当前 lock 内容解析出 `InstallPlan`，包含确定性的 `plan_id`、解析后的 pip install args、lock 内容快照及其哈希、解释器与创建时间。
- **FR-2 计划身份**：`plan_id` 对相同输入稳定；pip args、variant、lock 内容、python 解释器中任一变化都会产生不同 `plan_id`。
- **FR-3 隔离暂存**：venv 创建与依赖安装只发生在 `.meltano/<type>/<name>/staging/<plan_id>/` 下；提交前最终 venv 路径与其状态记录不被修改。
- **FR-4 可执行性检查**：暂存安装完成后、提交前必须验证：暂存 venv 的 python 可运行、插件可执行文件存在且可执行、以插件可执行文件发起一次探测（`--help`，带超时）成功；任一不满足则安装失败。
- **FR-5 原子提交**：通过同文件系统内的 rename 交换，将暂存 venv 切换到最终 venv 路径，并将候选「已提交状态记录」原子写入 `install.json`；提交后状态记录与 venv 指纹均指向该计划。
- **FR-6 失败保留旧版本**：计划解析之后任一步骤失败，最终 venv 与 `install.json` 仍指向上一个已提交的可运行版本；不提交半成品。
- **FR-7 恢复记录**：失败时写出机器可识别的恢复记录（位于 `recovery/`），至少含 `plan_id`、插件身份、失败阶段与原因、时间戳、暂存路径、上一个已提交计划标识与恢复建议。
- **FR-8 重启恢复**：开始某插件安装前扫描其暂存区：属于当前计划的暂存物予以复用（已完成可执行性检查的可直接进入提交；未完成的继续安装）；属于其他计划或损坏的暂存物予以清理；发现切换中途残留（如备份 venv）时恢复到上一可运行版本或完成切换。
- **FR-9 配置变更检测**：获得提交权后、真正切换前重新基于当前项目与 lock 解析计划；若当前 `plan_id` 与暂存计划不一致，拒绝提交旧计划、保留当前版本并写恢复记录。
- **FR-10 并发控制**：同一插件的跨进程安装通过 per-plugin 进程间锁串行化，且只有一个提交者；后到者提交前重新检查——若目标计划已被提交则走 SKIPPED 快速路径，若计划冲突则按 FR-9 拒绝；不同插件使用各自独立的锁，互不阻塞、并行安装。
- **FR-11 dry-run**：解析计划并报告每个插件「将安装/将跳过」及原因，不创建、不修改任何文件与目录。
- **FR-12 快速路径**：当 `install.json` 已提交记录的 `plan_id` 与当前解析计划一致、venv 的 python 存在且指纹匹配时，直接 SKIPPED，不创建暂存目录、不执行 pip。
- **FR-13 离线模式**：离线时不从 Hub 获取任何定义（lock 缺失即快速失败并给出可操作错误）；依赖安装只使用计划中已锁定且本地可用的制品——uv 后端使用 `--offline`，pip 后端使用 `--no-index`（可选 `--find-links`）；制品本地不可用时失败并保留旧版本。
- **FR-14 兼容旧项目**：没有 `install.json` 的既有项目视为「无已提交状态」，执行一次正常安装后建立状态记录；旧指纹文件 `.meltano_plugin_fingerprint` 继续被读取写入，运行时行为不变。
- **FR-15 hook 兼容**：`install` hooks（含 File 插件 `after_install`）在新版本成功提交后按现有语义触发；hooks 失败不回滚已提交的 venv，但产生 WARNING 状态（与当前语义一致）。

## Non-Functional Requirements
- **NFR-1 性能**：快速路径与 dry-run 不得产生 pip/uv 子进程；暂存复用避免重复下载与安装；注意 asyncio 开销（参考 AGENTS.md 引用的 PR #9724），不为每插件引入多余锁轮询。
- **NFR-2 跨平台**：原子操作使用 `os.replace`/同卷 rename 语义，兼容 macOS、Linux 与 Windows（Windows 下 venv 目录切换要求无进程占用）。
- **NFR-3 可观测性**：所有阶段（解析、暂存、检查、提交、恢复）通过 `structlog` 输出结构化日志，携带 `plugin_type`、`plugin_name`、`plan_id`、阶段字段；恢复记录路径在日志中可见。
- **NFR-4 可维护性**：以职责单一的新服务类承载（计划解析、暂存构建、提交、恢复），全部新代码带完整类型标注并通过 `mypy`/`ty`、`ruff`。
- **NFR-5 可测试性**：外部子进程与 Hub 访问可 mock；测试组织镜像源码结构，关键路径（失败保留、重启恢复、配置变更、并发、离线、dry-run）均有自动化测试。

## Constraints
- **Technical**: Python 3.10+；仅使用现有依赖（`fasteners`、`structlog`、`uv` 后端、`virtualenv` 后端）；不新增第三方依赖；遵循 `from __future__ import annotations` 与项目导入规范。
- **Business**: 不破坏 `meltano install/upgrade/add/run/elt/invoke/config/validate` 的现有 CLI 契约（新增选项必须有默认值且向后兼容）。
- **Dependencies**: lock 内容来自 `PluginLockService`；插件来自 `ProjectPluginsService`；venv 构建基于 `VirtualEnvService`/`VenvBackend`。

## Assumptions
- `.meltano` 与其 staging 目录位于同一文件系统，rename 为原子操作；若 `MELTANO_SYS_DIR_ROOT` 指向其他卷导致跨文件系统，提交器回退为「复制 + 校验」并在日志中标注（非原子窗口最小化），且测试覆盖同卷主路径。
- 可执行性探测统一使用 `--help`；不响应 `--help` 的插件属极少数，探测失败信息会说明如何报告问题。默认探测，不提供用户配置面（保持变更最小）。
- `MELTANO_OFFLINE` 真值语义：`1/true/yes/on`（大小写不敏感）视为离线。
- 恢复记录被后续成功安装标记为 `superseded` 而非删除，以满足「可识别、持久」要求。

## Acceptance Criteria

### AC-1: 安装计划具有确定性身份
- **Type**: `rule`
- **Given**: 同一项目的同一插件与同一份 lock 内容
- **When**: 连续两次解析安装计划
- **Then**: 两次得到相同的 `plan_id`，且 pip args、lock 哈希一致
- **Pass Condition**: 相同输入 `plan_id` 相等；改动 pip_url/variant/lock 内容/python 中任一项后 `plan_id` 改变
- **Evidence**: `tests/meltano/core/test_install_plan_service.py`（或对应镜像路径）中参数化用例断言

### AC-2: 依赖安装只发生在隔离暂存目录
- **Type**: `rule`
- **Given**: 插件存在已提交 venv，触发一次需要变更的安装
- **When**: 依赖安装阶段执行（mock pip/uv）
- **Then**: 安装命令的目标根路径为 `staging/<plan_id>/venv`；最终 venv 目录内容与状态记录在提交前未被修改
- **Pass Condition**: 断言 pip/uv 调用目标为暂存路径，且提交前最终 venv 路径 mtime/内容快照不变
- **Evidence**: 单元测试中对后端调用参数与最终路径快照的断言

### AC-3: 提交前必须通过可执行性检查
- **Type**: `rule`
- **Given**: 暂存 venv 已完成依赖安装
- **When**: 分别构造「python 可运行」「可执行文件缺失」「探测退出非 0」三种暂存环境
- **Then**: 仅全部检查通过才进入提交；其余情况安装失败且不切换
- **Pass Condition**: 三种失败构造均产生 ERROR 状态、最终 venv 不变，且不产生新的 `install.json`
- **Evidence**: 可执行性检查服务单元测试（用真实临时 venv + 伪造可执行脚本）

### AC-4: 提交为 venv 与状态记录的原子切换
- **Type**: `rule`
- **Given**: 暂存 venv 通过全部检查
- **When**: 提交执行
- **Then**: 最终 venv 路径内容等于暂存 venv，`install.json` 记录该 `plan_id`、pip args、lock 哈希与指纹；venv 指纹与记录一致
- **Pass Condition**: 提交后 venv python 可运行、指纹 == 计划指纹、`install.json` 反序列化字段全部匹配
- **Evidence**: 提交器单元测试 + 提交后 `VirtualEnv.read_fingerprint` 断言

### AC-5: 任一步失败保留上一可运行版本并写恢复记录
- **Type**: `rule`
- **Given**: 插件存在已提交的可运行版本（`install.json` + venv）
- **When**: 在「依赖安装」「可执行性检查」「切换」阶段分别注入失败
- **Then**: 最终 venv 与 `install.json` 仍指向上一个 `plan_id` 且 python 可运行；`recovery/` 下新增含 plan_id/阶段/原因/时间戳/暂存路径/上一计划的记录
- **Pass Condition**: 三阶段注入失败后旧版本可运行、旧状态记录不变，且恢复记录字段完整可解析
- **Evidence**: 事务编排服务参数化失败注入测试

### AC-6: 进程重启后复用同计划暂存物、清理过期暂存物
- **Type**: `rule`
- **Given**: 暂存目录下分别存在「与当前计划同 plan_id 且构建完整」「同 plan_id 构建不完整」「不同 plan_id」三种暂存物，以及一组切换中途残留（venv 已改名备份、新 venv 未就位）
- **When**: 新进程开始该插件安装
- **Then**: 完整同计划暂存物被复用并直接进入提交；不完整同计划暂存物继续安装；不同计划暂存物被删除；切换残留被恢复为上一可运行版本
- **Pass Condition**: 四种情形分别断言（复用无重复 pip 安装调用、继续安装发生、过期目录不存在、恢复后旧 venv 就位且可运行）
- **Evidence**: 恢复协调器单元测试

### AC-7: 等待期间配置改变则拒绝提交旧计划
- **Type**: `rule`
- **Given**: 暂存计划 P1 已构建完成；提交前项目配置或 lock 被改变，使当前解析计划为 P2
- **When**: P1 的提交器执行提交前复检
- **Then**: 提交被拒绝，当前版本保持不变，写出指向 P1 的恢复记录（原因含「计划已过期」）
- **Pass Condition**: venv/`install.json` 不变、暂存保留并记录、状态为 ERROR 且错误可操作
- **Evidence**: 提交器 stale-plan 测试

### AC-8: 同插件单提交者、不同插件并行
- **Type**: `rule`
- **Given**: 两个进程同时安装同一插件（相同/不同计划各一组），以及两个进程分别安装不同插件
- **When**: 并发执行安装
- **Then**: 同插件只有一个提交发生；第二个提交者发现已提交则 SKIPPED，计划冲突则拒绝；不同插件两者均成功提交且互不等待
- **Pass Condition**: 同插件最终只有一个 `install.json` 提交者（另一状态为 SKIPPED/ERROR），不同插件用例总耗时接近单次安装（锁不交叉）
- **Evidence**: 使用 per-plugin 锁的并发测试（asyncio + 线程/子进程模拟跨进程）

### AC-9: dry-run 不写磁盘
- **Type**: `rule`
- **Given**: 任意项目状态
- **When**: 以 dry-run 运行安装
- **Then**: 输出每个插件的计划动作与原因；项目目录（含 `.meltano`、`plugins/`）前后文件树完全一致，且无子进程产生
- **Pass Condition**: 目录树 diff 为空、pip/uv 调用为零
- **Evidence**: CLI/服务级 dry-run 测试（前后快照对比）

### AC-10: 无变更时保持快速路径
- **Type**: `rule`
- **Given**: `install.json` 的 `plan_id` 与当前计划一致、venv python 存在、指纹匹配
- **When**: 以任意 reason 运行安装
- **Then**: 状态为 SKIPPED，无暂存目录创建、无 pip/uv 子进程
- **Pass Condition**: 无后端调用、状态 SKIPPED 且消息说明无需变更
- **Evidence**: 快速路径单元测试

### AC-11: 离线模式只使用已锁定且本地可用制品
- **Type**: `rule`
- **Given**: 离线标志生效
- **When**: 分别执行「lock 已存在且制品在本地缓存/路径可用」「lock 缺失」「制品本地不可用」三种场景
- **Then**: 无 Hub/网络访问；uv 调用含 `--offline` 或 pip 调用含 `--no-index`；lock 缺失与制品不可用均快速失败、旧版本保留并给可操作错误
- **Pass Condition**: 断言后端参数、无 Hub 调用、失败场景旧版本不变
- **Evidence**: 离线解析与构建参数单元测试

### AC-12: 失败错误信息可操作性
- **Type**: `rubric`
- **Dimension**: 错误信息是否说明失败原因并给出用户可执行的下一步（重试/清理暂存/移除离线/检查 lock）
- **Scale**: 1-5
- **Anchors**: 1 = 裸异常、无上下文；3 = 说明原因但无下一步；5 = 原因 + 明确下一步 + 相关路径/计划标识
- **Pass Threshold**: >= 4
- **Evidence**: 审查所有新定义错误类与失败用例的 message/instruction 文本

### AC-13: 服务设计与代码质量
- **Type**: `rubric`
- **Dimension**: 新服务类职责单一、与现有核心风格一致、类型标注完整、无多余 asyncio 开销
- **Scale**: 1-5
- **Anchors**: 1 = 逻辑堆叠在既有方法中、无类型；3 = 有拆分但边界模糊；5 = 解析/暂存/提交/恢复职责清晰、全标注、通过 lint 与类型检查
- **Pass Threshold**: >= 4
- **Evidence**: 代码结构评审 + `nox -t lint` 与 `nox -s typing` 结果

### AC-14: 自动化测试覆盖关键事务路径
- **Type**: `rubric`
- **Dimension**: 测试覆盖 AC-1~AC-11 的关键路径且测试稳定（无真实网络、子进程可 mock）
- **Scale**: 1-5
- **Anchors**: 1 = 仅 happy path；3 = 主要路径覆盖但失败/恢复缺失；5 = 成功、各阶段失败、重启、配置变更、并发、离线、dry-run 全覆盖且通过
- **Pass Threshold**: >= 4
- **Evidence**: 新增/修改测试清单与 `pytest` 运行结果

## Open Questions
- 无（三个关键设计点已经用户确认；其余按 Assumptions 执行）。
