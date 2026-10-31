# Temporal 能力验证 · 实测结果

> 2026-10-31 · Windows · Temporal CLI 1.9.1 (Server 1.32.0) · `temporalio` 1.35.0 · Python 3.14.10
> 代码：`cae-demo/demo.py`（宿主）+ `cae-demo/flows.py`（workflow/activity）

## 一、部署形态（V1）

```
temporal server start-dev --db-filename .\temporal.db --ui-port 8233
    ↑ 单进程：frontend + history + matching + worker + UI，SQLite 持久化

worker 1: --queues orchestrator   pid 95xx     跑 workflow 代码
worker 2: --queues host-a         pid 32440    跑 activity
worker 3: --queues host-b         pid 26xxx    跑 activity
```

**结论**：分布式执行面成立。三个独立进程，通过 task queue 分流，server 单点不影响执行分发。

---

## 二、九个场景

| # | 场景 | 结果 | 证据 |
|---|---|---|---|
| S1 | DAG 编排（1→(2,3)→4） | ✅ | 执行顺序 `s1,s2,s3,s4`，依赖正确 |
| S2 | 按主机/工具分发 | ✅ | 4 个步骤分别落到 host-a / host-b，涉 2 个进程 |
| S3 | 暂停/恢复 | ✅ | 暂停期间新增调用 **0**，恢复后跑完 6 步 |
| S4 | 失败自动重试 | ✅ | 3 次尝试（attempt 1/2/3），最终 WorkflowFailureError |
| S5 | 超时中断 | ✅ | start_to_close=1s 而任务跑 5s → 失败 |
| S6 | 120 步循环 + 历史保护 | ✅ | 120 次调用 / **4 次执行**（continue-as-new） |
| S7 | 运行中改图 | ✅ | 旧实例 4 步、新实例 5 步，互不影响 |
| S8 | 大 payload | ✅（预期失败） | 3MB 返回值 → 直接失败（默认上限 2MB） |
| S9 | **杀 worker 后恢复** | ✅ | 见下 |

### S9 细节（最关键）

6 步串行，每步 2 秒。跑到第 6 步（k5）时**杀掉 host-a worker 进程**，4 秒后重启。

```
idem                attempt   pid
s9-kill-2:k0            1      32440
s9-kill-2:k1            1      32440
s9-kill-2:k2            1      32440
s9-kill-2:k3            1      32440
s9-kill-2:k4            1      32440
s9-kill-2:k5            1      32440     ← 杀在这里，正在飞
s9-kill-2:k5            2      26356     ← 新 worker 重试一次
```

最终 `Status = COMPLETED, steps = 6`。

**结论：已完成的步骤零重跑；只有"正在飞"的那一个被重试了一次。** 这就是 at-least-once 的准确边界。

---

## 三、三个必须记住的结论

### 1. 任务分发是「运行时」决定的，不是定义时写死

```python
await workflow.execute_activity(dispatch, args=[...], task_queue=f"host-{host_id}")
```

`task_queue` 是**调用参数**，可以按数据算出来。Airflow 的 `queue=` 在 DAG 定义时写死。
→ **按工具/主机定向分发，Temporal 比 Airflow 强，不是弱。** 专家 B 的疑问有正面答案。

部署形态两种都行：
- 一机一队列一 worker（求解器与 license 绑机器）
- 一 worker 监听多队列（中心化，省进程）

### 2. 「中断续跑」的真实语义是 at-least-once，不是 exactly-once

- 已完成的 activity：结果在历史里，**永不重跑**
- 正在飞的 activity：worker 死后超时，**重试一次**，可能重复执行

所以 6 端点的每个动作都**必须幂等**。这不是建议，是硬要求。
（`run` 提交作业尤其危险：重试一次 = 提交两个作业。）

### 3. 循环必须有 continue-as-new，否则历史爆炸

S6：120 步、每 25 步切一次 → 4 次执行链，工作流正常完成。
如果不切，120 步 ≈ 360+ 条事件，几千轮的闭环迭代必然撞 5 万条上限。

写法要点：阈值要按「**本次执行新增的步数**」算，不能按总步数算——否则 continue 之后新执行一上来又满足条件，无限续跑（我第一版就踩了这个坑）。

---

## 四、其他实测发现

| 发现 | 说明 |
|---|---|
| 一次最简执行 = 11 条事件 | 2 次 workflow task + 1 次 activity，开销约 130ms |
| 3MB 返回值直接失败 | 默认 blob 上限 2MB，产物必须走 S3 + claim-check |
| 暂停后 worker 空转 | 暂停期间 Worker 不占资源，进程仍在但无任务 |
| workflow 代码沙箱 | `pathlib.Path.resolve`、`os.environ` 在 workflow 里被禁，只能放 activity |
| `execute_activity` 多参数 | 必须用 `args=[...]`，不能位置参数直传 |
| 信号可发给已完成的 workflow | 不报错、无效果，要在 UI/API 层做状态校验 |

---

## 五、没验证的（注意边界）

| 项 | 为什么没做 |
|---|---|
| server 重启后的恢复 | 会打断你正在跑的 dev server，没动 |
| 多机真分布式 | 单机多进程已能证明分发逻辑，跨机只是网络位置不同 |
| Elasticsearch 可见性 | dev server 没有，需要全量部署 |
| 高并发压测 | dev server 单进程，压测结果没有参考意义 |
| 归档 / 保留期 | 同上 |

---

## 六、复现步骤

```powershell
# 1. 服务端（单独窗口，保持运行）
cd C:\Users\admin\temporal-demo
temporal server start-dev --db-filename .\temporal.db --ui-port 8233

# 2. 三个 worker（各自一个窗口）
cd C:\Users\admin\Documents\deepseek-harness\default-workspace\cae-demo
python demo.py worker --queues orchestrator
python demo.py worker --queues host-a
python demo.py worker --queues host-b

# 3. 跑 8 个场景
python demo.py run

# 4. S9：单独跑，中途手动杀掉 host-a 的 worker 窗口
python s9_start.py
```

结果落在 `data/report.json`，调用流水在 `data/ledger.jsonl`（每行一次工具调用）。
