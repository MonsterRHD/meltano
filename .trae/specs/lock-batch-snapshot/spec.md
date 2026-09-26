# meltano lock 批次快照 - 产品需求文档

## Overview

- **Summary**: 为 `meltano lock` 增加批次快照（batch snapshot）能力：一次批次从选定 environment 的有效配置解析全部目标插件，逐项冻结 Hub/本地定义、来源与继承来源，全部解析完成后一次原子发布带版本标识的锁快照；下游读取方只能看到旧快照或完整新快照。
- **Purpose**: CI 同时为一组 extractor、loader、mapper 更新锁定义时，下游运行不能读到只更新了一部分插件的项目状态；并支持失败逐项重试、并发冲突检测、中断清理/恢复，以及离线下的来源一致性。
- **Target Users**: CI/CD 流水线维护者、Meltano 项目开发者。

## Goals

- 新增 `meltano lock --batch`：目标集合来自选定 environment 的有效插件配置（沿用名称与 `--plugin-type` 过滤）。
- 逐项解析并冻结：插件独立定义（StandalonePlugin 内容）、定义来源（Hub origin/ref 或本地）、variant 元数据，以及继承/自定义插件的继承来源信息。
- 全部目标解析成功后，一次发布带 `snapshot_id` 的锁快照；快照指针原子切换，读取方要么读到旧快照，要么读到完整新快照。
- 新快照必须完整携带未作为本次目标的旧有锁定条目（旧快照条目或旧散列锁文件）。
- 单个插件解析失败时保留旧快照，输出逐项结果；重试只复用输入仍一致的成功解析。
- 并发批次在发布时以指针与项目版本检测冲突。
- 进程在暂存或发布中断后，后续进程能够完成同一快照，或清理残留。
- 离线执行不得把缓存中来源不同的定义混入同一快照。
- 现有单插件 lock、只读检查、不含锁文件的兼容项目继续按原方式工作。

## Non-Goals

- 不改变 `meltano.yml` 的插件配置语义，不改变 environment 配置与插件的合并/继承语义。
- 不提供远程或云端托管的锁存储；快照仍位于项目目录内，可随 git 提交。
- 不引入新的网络级 Hub 定义缓存；来源一致性基于现有 Hub 索引缓存、暂存区与快照元数据实现。
- 不改变 `meltano run`、`meltano install` 等命令的调用方式与参数。
- 不改变当前“自定义插件与被继承插件不产生独立锁文件”的基本规则（仅在快照清单中记录其拓扑/继承来源）。

## Background & Context

- 当前写入路径：[lock.py](file:///Users/ding/Documents/swe/09224/project-09/src/meltano/cli/lock.py) 逐个插件调用 `PluginLockService.save`，把定义写入项目根下的散文件 `plugins/<type>/<name>[--<variant>].lock`。
- 当前读取路径：`LockedDefinitionService` → [PluginLockService.load_content](file:///Users/ding/Documents/swe/09224/project-09/src/meltano/core/plugin_lock_service.py#L192-L215) 直接读取对应散文件，文件缺失时即时从 Hub 拉取；`get_standalone_data` 供清单（manifest）生成使用。
- 逐文件写入/更新期间，另一个进程可能在任意时刻读到“部分插件已更新、部分仍旧”的状态。
- 项目已具备：`fasteners.InterProcessLock` 进程间锁、structlog 日志、按 Hub URL hash 分文件的 Hub 索引缓存、`ProjectReadonly` 只读错误、`MeltanoError` 用户可操作错误体系。

## Functional Requirements

- **FR-1（批次目标解析）**: `meltano lock --batch` 从当前激活 environment（可为空）的有效插件配置中取得全部插件，按插件名参数与 `--plugin-type` 过滤，得到本次批次目标；根插件、继承插件、自定义插件均可被识别与分类。
- **FR-2（逐项冻结）**: 对每个可锁定的根插件，冻结其独立定义内容、variant 元数据（默认/弃用）与来源信息（`kind=hub` 时记录 Hub origin 与定义 ref URL；本地来源记录本地来源标识）；继承插件记录 `inherit_from`，自定义插件记录 custom 标记。
- **FR-3（暂存与指纹）**: 解析结果先写入项目 sys 目录下的暂存区，每项带输入指纹（插件类型、根名称、variant、Hub origin，本地定义再含其规范化内容）与状态；暂存区记录 pid 与阶段状态。
- **FR-4（原子发布）**: 全部目标解析成功后，组装自包含的快照目录 `plugins/snapshots/<snapshot_id>/`，再通过临时文件 + `os.replace` 原子切换指针文件 `plugins/lock.snapshot.json`；切换完成前读取方不会看到新快照。
- **FR-5（完整携带）**: 未作为本次目标的旧快照条目按原样携带进新快照；尚无指针时，已有的散列锁文件作为 legacy 本地条目纳入首个快照；新快照相对其清单始终完整。
- **FR-6（失败保留与逐项报告）**: 任一目标解析失败时不得发布，旧指针保持不变；命令输出逐项结果（locked/reused/failed/skipped 及失败原因）并以非零退出。
- **FR-7（重试复用）**: 后续批次从最近的暂存记录中复用“成功且输入指纹一致”的解析结果（不重复请求 Hub），失败项与指纹变化项重新解析；发布成功后暂存清理。
- **FR-8（并发冲突检测）**: 发布在进程间锁内进行；若当前指针的 `snapshot_id` 或项目版本与批次基线不一致（被其他批次抢先发布或 `meltano.yml` 已变更），本次发布以冲突错误终止且不覆盖，暂存保留供重新执行。
- **FR-9（中断恢复）**: 批次启动时执行恢复扫描：原进程已死、快照已完整组装（staged/publishing）且指针与项目版本未变时，完成同一快照的发布；未完成组装或基线已变化时清理该暂存；活进程的暂存不动；无引用快照目录被清理。
- **FR-10（读取方快照感知）**: 存在指针时，锁读取从指针所指快照中读取对应条目（含 variant 默认匹配与 variant 元数据）；清单未列出的插件回退到原有散文件/Hub 拉取路径；无指针时行为与现状完全一致。
- **FR-11（离线来源一致性）**: 暂存复用与快照携带必须校验 Hub origin 与当前配置一致；origin 改变后相关条目不被复用/携带而需重新解析；离线无法重新解析时批次失败，不发布含混合 origin 的快照；清单逐项记录来源。
- **FR-12（CLI 兼容）**: 不带 `--batch` 时，单插件锁定、`LockfileAlreadyExistsError`、`--update` 行为不变；已存在快照且未带 `--update` 的批次报错；只读项目写入得到 `ProjectReadonly`，读取不受影响。

## Non-Functional Requirements

- **NFR-1（兼容性）**: 不新增必需的运行时依赖；现有测试在不改动既有断言语义的前提下全部通过。
- **NFR-2（可维护性）**: 批次逻辑以新的核心服务类承载，CLI 仅负责参数与输出；新代码全部带类型注解，满足 ruff/mypy/ty 检查。
- **NFR-3（可观测性）**: 逐项结果、冲突、恢复清理均通过 structlog 输出；错误使用 `MeltanoError` 子类并给出可操作指引。
- **NFR-4（性能）**: 复用成功项时不得产生对应 Hub 请求；不引入无界的残留目录增长（发布后清理无引用快照）。

## Constraints

- **Technical**: Python 3.10+；锁文件为 JSON；原子切换依赖同文件系统上的 `os.replace`；跨进程协调使用 fasteners；锁与快照位于项目根 `plugins/`，暂存位于 `.meltano`（gitignored）。
- **Business**: 必须保持对现有项目（含无锁文件项目、散列锁项目）的向后兼容。
- **Dependencies**: 依赖现有 `MeltanoHubService`（含其索引缓存）、`ProjectPluginsService`、`ProjectDirsService`，不改动其对外接口。

## Assumptions

- “项目版本”定义为 `meltano.yml` 文件内容的 SHA-256；该文件变更（含 environment 配置变更）即视为项目版本变化。
- 快照清单中记录的文件路径均为相对项目根的路径；快照目录自包含，不依赖旧快照中的文件字节。
- 同一文件系统内 `os.replace` 对读取方为原子操作；读取方通过指针间接定位快照，因此不会观察到半发布状态。
- 测试环境中的 Hub 由现有 MockAdapter 提供，可通过请求计数验证复用行为。

## Acceptance Criteria

### AC-1: 批次目标来自选定 environment 的有效配置

- **Type**: `rule`
- **Given**: 项目包含若干 extractor/loader/mapper，其中含根插件、继承插件与自定义插件，并可激活指定 environment
- **When**: 执行 `meltano lock --batch`，可附带 `--plugin-type` 与插件名参数
- **Then**: 批次目标为有效配置中经名称/类型过滤后的全部可锁定根插件，继承/自定义插件被识别并分类记录
- **Pass Condition**: 目标清单与过滤预期逐项一致；报告中可见根/继承/自定义分类
- **Evidence**: 新增服务层与 CLI 测试，断言目标集合与过滤结果

### AC-2: 逐项冻结定义、来源与继承来源

- **Type**: `rule`
- **Given**: 批次已识别目标与非目标插件
- **When**: 逐项解析
- **Then**: 每个根插件产出独立定义内容、variant 元数据与 provenance（hub: origin+ref；或本地来源标识）；继承插件条目含 `inherit_from`；自定义插件含 custom 标记
- **Pass Condition**: 暂存项与快照清单中每个条目包含上述字段，且定义内容可独立解析回 StandalonePlugin
- **Evidence**: 服务层测试读取暂存/清单内容并反序列化校验

### AC-3: 带版本标识快照的原子发布

- **Type**: `rule`
- **Given**: 全部目标解析成功、快照目录已组装
- **When**: 发布执行
- **Then**: 快照位于 `plugins/snapshots/<snapshot_id>/`，指针 `plugins/lock.snapshot.json` 经临时文件 `os.replace` 切换；切换前指针不指向新快照，切换后清单所列文件全部存在且可解析
- **Pass Condition**: 发布后清单中 100% 条目文件存在且 JSON 合法；读取调用在切换前后分别只返回旧快照或新快照内容
- **Evidence**: 测试断言文件完整性、指针内容与读取结果

### AC-4: 新快照完整携带未变更条目

- **Type**: `rule`
- **Given**: 旧指针快照（或无指针但存在散列锁文件），本次批次只针对部分目标
- **When**: 发布新快照
- **Then**: 非目标旧条目按原样携带（散列锁作为 legacy 本地条目），目标条目为新解析结果；新快照相对其清单完整
- **Pass Condition**: 新快照条目集合 = 携带条目 ∪ 目标条目；携带条目与旧快照对应文件字节一致
- **Evidence**: 服务层测试对比条目集合与文件字节

### AC-5: 单项解析失败保留旧快照并逐项报告

- **Type**: `rule`
- **Given**: 批次中至少一个目标插件解析抛出异常
- **When**: 批次执行结束
- **Then**: 指针 `snapshot_id` 不变；逐项报告每项状态（含失败项标识与错误原因）；命令以非零退出码结束
- **Pass Condition**: 指针前后一致；报告可定位失败项；CLI 退出码为 1
- **Evidence**: 以 Hub 返回错误的插件触发批次的测试

### AC-6: 重试按输入指纹复用成功解析

- **Type**: `rule`
- **Given**: 一次批次部分成功部分失败（或全部成功后用户重试）
- **When**: 再次执行同一批次，或改变某项输入（variant/Hub origin/本地定义）后执行
- **Then**: 成功且指纹一致的项直接复用、不产生 Hub 请求；失败项与指纹变化项重新解析
- **Pass Condition**: Hub 请求计数显示复用项无新请求；指纹变化项产生新解析
- **Evidence**: 基于 MockAdapter 请求计数与暂存状态的测试

### AC-7: 并发批次冲突检测

- **Type**: `rule`
- **Given**: 两个批次基于同一旧指针并发执行
- **When**: 批次 A 完成发布后，批次 B 进入发布
- **Then**: B 检测到指针 `snapshot_id`/项目版本与其基线不一致，得到冲突错误且不覆盖指针，B 的暂存保留
- **Pass Condition**: B 抛出冲突类型错误；指针仍指向 A 的快照；B 暂存目录仍存在
- **Evidence**: 服务层构造两个暂存并连续发布的测试

### AC-8: 暂存或发布中断后可完成或清理

- **Type**: `rule`
- **Given**: 存在带 pid 与阶段状态（resolving/staged/publishing）的暂存目录，以及无引用快照目录
- **When**: 后续批次执行恢复扫描
- **Then**: 死 pid + staged/publishing + 指针与项目版本未变 → 完成发布；死 pid + resolving 或基线已变 → 清理；活 pid 暂存保留；无引用快照目录被清理
- **Pass Condition**: 上述四种情形各自产生规定结果
- **Evidence**: 使用不存在 pid 构造各状态暂存的恢复测试

### AC-9: 现有单插件 lock、只读检查与无锁项目兼容

- **Type**: `rule`
- **Given**: 分别为散列锁项目、只读项目、从无锁文件的项目
- **When**: 执行原有 `meltano lock`（不带 `--batch`）、批次写入及下游读取
- **Then**: 单插件锁定/已存在错误/`--update` 行为不变；只读项目批次写入得到 `ProjectReadonly` 而读取正常；无锁项目按原路径（缺锁即回退/Hub 拉取）工作
- **Pass Condition**: 既有 lock 相关测试全部通过；新增只读与无锁场景测试通过
- **Evidence**: pytest 既有 lock 测试结果与新增测试

### AC-10: 离线时不混合不同来源的定义

- **Type**: `rule`
- **Given**: 暂存或旧快照中存在 origin A 的成功条目，项目当前配置 origin B，且网络不可达
- **When**: 执行批次（复用/携带并组装）
- **Then**: origin 不一致的 hub 条目不被复用/携带，而要求重新解析；离线无法解析时批次失败、指针不变；快照清单逐项记录 origin
- **Pass Condition**: 旧条目不出现在新快照；离线批次以错误退出且指针不变；清单中 origin 字段可核验
- **Evidence**: origin 变更 + Hub 连接失败组合场景的服务层测试

### AC-11: 实现与项目架构约定的一致性

- **Type**: `rubric`
- **Dimension**: 架构符合度（服务类边界、类型注解、日志与错误类型、测试结构镜像源码）
- **Scale**: 1-5
- **Anchors**: 1 = 批次逻辑散落在 CLI、无明确边界；3 = 有服务类但与既有服务耦合、测试覆盖一般；5 = 独立核心服务类、完整类型注解、structlog/MeltanoError 用法规范、测试镜像源码结构
- **Pass Threshold**: >= 4
- **Evidence**: 代码结构审查与 lint/type 检查输出

## Open Questions

- 无（上述 Assumptions 已明确关键取舍；如实现中发现新歧义将回到本阶段更新）。
