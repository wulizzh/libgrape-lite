## 背景

第三章完整实验在修复 `wcc` 后继续重跑时，又在 `sssp / web-Google / Feedback` 处中断。服务器日志显示 rank 1 在预划分完成后抛出 `std::bad_alloc`。

## 本次修复

- 对 `sssp` / `bfs` / `pagerank` / `wcc` 的 runtime-feedback 候选规划增加统一门控：
  - 只有当 feedback 控制已经被触发时，才执行候选紧急度计算与排序。
  - 当控制尚未触发时，保留原先“全量直接处理”的路径，不进行额外规划。

## 设计考虑

- 在 feedback 开关打开但控制尚未激活的早期轮次里，排序不会改变处理预算，因为这时仍然是“全部处理”。
- 对大候选集提前做排序只会增加内存和 CPU 压力，尤其容易在 `sssp` 这类私有候选密集的首轮上放大开销。
- 这次改动不改 TA、不改图划分层，只收缩 CA 侧 runtime-feedback 的启用时机，更符合“先观测、后干预”的第三章思路。

## 后续验证

- 重新编译 `analytical_apps`
- 先回归 `sssp / web-Google / Feedback`
- 通过后再重启第三章完整实验
