## 背景

第三章完整实验在 `sssp / web-Google / Feedback+Scheduling` 处中断，但同样参数在手动回归、且 `out_prefix` 不含 `+` 字符时可以正常跑通。

## 本次修复

- 调整 `scripts/run_chapter3_runtime_bench.sh` 中的 `sanitize_name()`：
  - 先将 `+` 替换为 `Plus`
  - 再把其余非 `[A-Za-z0-9._-]` 字符替换为 `_`

## 设计考虑

- 这次改动不改变 CSV 里的模式名称，仍然保留 `Feedback+Scheduling` 作为实验模式标签。
- 只收敛输出目录和日志文件路径的命名，避免路径中出现 `+` 导致底层某些运行路径异常。
- 这样能最小代价恢复第三章完整实验脚本的稳定性。

## 后续验证

- 用脚本重新发起第三章完整实验
- 确认 `FeedbackPlusScheduling` 路径下结果正常落盘
