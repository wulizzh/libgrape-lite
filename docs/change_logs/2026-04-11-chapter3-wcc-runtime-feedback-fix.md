## 背景

第三章完整实验在 `wcc / wiki-topcats / Feedback+Scheduling` 处中断。服务器日志显示主实验脚本在 `WCCContext::MergeDeferredPrivateCandidates()` 附近异常退出，导致整轮 Chapter 3 benchmark 只写入前 24 行结果。

## 本次修复

- 将 `wcc` 的“新产生私有候选”和“延后私有候选”统一收敛到同一套候选缓存中，不再做额外的 deferred-to-active 搬运。
- 保留 `deferred_private_rounds` 作为 backlog 计数与协同调度触发依据。
- `ProcessPrivateCandidates()` 的 secure-memory 代理值改为直接使用当前候选规模，避免重复叠加 backlog。
- 更新 `PEval` / `IncEval` / `PropagateLabelPull` / `PropagateLabelPush` 的基线统计，去掉对 `MergeDeferredPrivateCandidates()` 的依赖。

## 设计考虑

- 这一修复不改 TA，不改图划分层，只调整 CA 侧 `wcc` 的 runtime-feedback 缓存策略。
- 统一缓存后，新一轮到来的私有候选可以直接与上一轮延后候选合并取最优值，更贴近“运行时反馈下的增量调度”思路。
- 这样也避免了 `wcc` 在高负载 `Feedback+Scheduling` 路径上进行额外状态搬运时的崩溃点。

## 后续验证

- 重新编译 `analytical_apps`
- 先单独回归 `wcc / wiki-topcats / Feedback+Scheduling`
- 通过后恢复第三章完整实验与自动绘图
