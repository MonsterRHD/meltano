# meltano lock 批次快照 - Independent Review

- [ ] CP-R1: 批次目标来自选定 environment 有效配置
  - **Type**: `rule`
  - **Covers**: AC-1
  - **Evidence**: Pending（Review R1: pass — 独立探针与代码阅读）

- [ ] CP-R2: 逐项冻结定义、来源与继承来源
  - **Type**: `rule`
  - **Covers**: AC-2
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R3: 带版本标识快照的原子发布
  - **Type**: `rule`
  - **Covers**: AC-3
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R4: 新快照完整携带未变更条目（含同名其他 variant）
  - **Type**: `rule`
  - **Covers**: AC-4
  - **Evidence**: Pending（Review R1: **fail** → F1）

- [ ] CP-R5: 单项失败保留旧快照并逐项报告
  - **Type**: `rule`
  - **Covers**: AC-5
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R6: 重试按输入指纹复用成功解析（跨进程）
  - **Type**: `rule`
  - **Covers**: AC-6
  - **Evidence**: Pending（Review R1: **fail** → F2）

- [ ] CP-R7: 并发批次冲突检测
  - **Type**: `rule`
  - **Covers**: AC-7
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R8: 暂存或发布中断后可完成或清理
  - **Type**: `rule`
  - **Covers**: AC-8
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R9: 单插件 lock / 只读检查 / 无锁项目兼容
  - **Type**: `rule`
  - **Covers**: AC-9
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-R10: 离线时不混合不同来源的定义
  - **Type**: `rule`
  - **Covers**: AC-10
  - **Evidence**: Pending（Review R1: pass）

- [ ] CP-U1: 架构符合度
  - **Type**: `rubric`（1-5，阈值 >= 4）
  - **Covers**: AC-11
  - **Evidence**: Pending（Review R1: 4/5 pass）

## Review History

### Review R1

- **Result**: fail
- **Summary**: 独立读码、独立运行测试（随机/固定排序、多随机种子）与 ruff/mypy/ty；6 组探针覆盖未测边界。2 个 actionable（F1、F2，均 high），5 条 advisory（F3-F7），无 blocked。
- **Evidence**:
  - 测试：42 + 10 + 5 + 69（1 xfailed）passed（详见 tasks/返回报告）
  - 静态检查：ruff check/format pass；mypy/ty 与改动相关文件无错误
  - F1 探针：双 variant 基线快照、单 variant 更新批次后 singer-io 条目丢失
  - F2 探针：死 pid 暂存被 recover 删除，重试时成功项重新请求 Hub
