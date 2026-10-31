# 用 Temporal 承载「流程设计引擎 + 微服务编排平台」——设计备忘

> 版本 v0.1 · 2026-10-30
> 用途：你离线期间的阅读材料。结论在前，细节在后，最后是分阶段 Demo 计划和验证清单。
> 所有"我替你假设了"的地方都用 **[假设]** 标出，回来逐条确认即可。

---

## 0. 一页纸

你的需求，翻译成一句话：

> **把「工具 → 工作流 → 任务」三层结构，做成一个可编辑、可版本化、可暂停续跑、可对外暴露执行入口的平台；执行内核用 Temporal。**

三个判断，先说结论：

1. **Temporal 能覆盖"调度能力变强"这部分，而且是最硬的部分**——持久化、重试、超时、暂停、长等待、可观测。这块它比 Airflow 强一档，因为 Airflow 的实例是"按时间表跑一次"，你的是"每个业务对象一条长命流程"。

2. **Temporal 不能覆盖"平台"这部分**——定义管理、版本、创建人/时间、路径暴露、executions/webhooks 页面、NL 生成，全都要自己做。Temporal 只提供执行内核 + 一个很弱的事件时间线视图，**没有 DAG 图，没有业务元数据**。

3. **真正的难点不是 Temporal，是"DAG 是用户数据、而 Temporal 的代码是编译期固定的"这个矛盾**。解法只有一个：写一个**通用解释器 Workflow**，把用户的图当输入数据，在启动瞬间**快照 + 版本锁定**，然后在解释器里按拓扑序执行。这个快照机制是整个平台的地基，做错了会在"运行中改定义"时炸得很惨。

一句话风险提示：**如果你的团队不能接受"工作流代码必须确定性"这条纪律，Temporal 会变成负债。** 这是选型的第一道门槛，不是细节。

---

## 1. 需求 → 技术语言的翻译

先把你描述的东西对齐到可实现的概念，避免后面各说各话。

| 你的说法 | 技术对象 | 备注 |
|---|---|---|
| 流程设计引擎 | Workflow 定义器（可视化编排 + 存储） | 产出是**数据**（图 JSON），不是代码 |
| 工作流内的"工具" | Tool（带类型/入参 schema/超时/重试的执行单元） | 归一化的落点在这里 |
| 工作流 | Workflow Definition（工具组成的图） | 有版本、有创建人/时间 |
| 工作流跑在对应路径下 | HTTP facade：`POST /flows/{path}` → 启动一次执行 | 对外契约，不是执行机制 |
| 微服务管理平台 | Task 定义器（工作流组成的图） | "一条线"是 DAG 的退化情形 |
| 任务 | Task Definition（工作流引用 + 依赖边） | 同样要版本 |
| 启动/停止/续跑 | TaskRun 的生命周期 + pause/resume 信号 | 见 §4-D4，语义要钉死 |
| executions 页 | 执行实例列表（两类：workflow run / task run） | 见 §4-D6，不能只靠 Temporal |
| webhooks 页 | 入站触发器（外部系统打进来启动流程） | 见 §10 |

### 三层结构

```
Tool         ← 最底层，真正干活的（HTTP 调用 / 脚本 / LLM / 内置算子）
  ↑ 被编排
Workflow     ← 工具组成的图，对外暴露成一条路径
  ↑ 被编排
Task         ← 工作流组成的图，有启动/停止/续跑
```

**关键点：这三层是同一套机制的递归。** Workflow 是"图 + 工具节点"，Task 是"图 + 工作流节点"。所以解释器只写一次，节点类型可扩展。这是你"微服务设计归一化"最省力的实现路径。

---

## 2. 我替你假设的三个前提（请回来确认）

**[假设 A] "工作流跑在对应的路径下" = 每个工作流有一个 URL 路径，外部系统通过它触发执行。**
如果你的意思是"工作流的代码部署在某个路径下"，那设计会简单很多（不需要 facade），但 webhooks 页就没意义了。我按前者设计。

**[假设 B] "停止后从停止处继续" = 暂停（pause），不是取消（cancel）。**
这两者天差地别：
- **暂停**：执行实例还活着，状态冻结，恢复时从断点继续。Temporal 原生支持，成本几乎为零。
- **取消**：执行终止，回不来了。想"续"只能重新跑。
你说的是暂停。但真实需求里往往混着"这个任务卡死了我要掐掉"——那是 terminate。**三种语义必须分开做，否则用户会误用。**

**[假设 C] 步骤的暂停粒度 = 步骤边界，不是步骤内部。**
即：暂停后，正在跑的那一步会跑完（或按配置被取消），不会"停在 HTTP 请求发出去一半的地方"。这个假设必须写进产品文档，否则用户会期待"精确到行"的续跑，而那在技术上对大多数工具是不可能的。

---

## 3. 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│  控制面（你自己写）                                           │
│  ├─ Web UI：Workflows / Tasks / Executions / Webhooks         │
│  ├─ API（FastAPI）：CRUD、版本、启动、暂停、查询               │
│  ├─ 定义存储（Postgres）：tool / workflow / task / run 元数据  │
│  └─ 执行投影（Postgres）：从 Temporal 抽取的执行索引与步骤状态  │
└───────────────┬─────────────────────────────────────────────┘
                │ start / signal / query / list
┌───────────────▼─────────────────────────────────────────────┐
│  执行面（Temporal）                                           │
│  ├─ Temporal Server（dev: 单进程 SQLite；prod: PG/Cassandra）  │
│  ├─ Worker: orchestrator 队列 → TaskFlow / ToolFlow 解释器     │
│  ├─ Worker: tools-http 队列  → HTTP 工具                      │
│  ├─ Worker: tools-script 队列 → 沙箱脚本（隔离容器）           │
│  └─ Worker: tools-llm 队列   → 模型调用（限流、超时独立）       │
└───────────────┬─────────────────────────────────────────────┘
                │
┌───────────────▼─────────────────────────────────────────────┐
│  被编排的世界：外部 HTTP 服务、数据库、对象存储、模型 API      │
└─────────────────────────────────────────────────────────────┘
```

**为什么要按能力拆多个 task queue**：工具类型不同，资源特征完全不同。HTTP 工具是 IO 密集、可以几百并发；脚本工具要沙箱、要限制 CPU/内存；LLM 工具要限流、要长超时、要单独计费。混在一个队列里，长任务会把短任务饿死。这是 practice 里最容易忽略、最早爆炸的一点。

---

## 4. 八个关键设计决策

每个决策给：选项 → 推荐 → 理由 → 代价。

### D1 · 两层是否都用 Temporal？

| 选项 | 说明 |
|---|---|
| A. 只用 Temporal 做 Task 层，Workflow 层用进程内执行器 | 简单，但 Workflow 层的持久化、重试、可观测要自己造 |
| B. **两层都用 Temporal（Workflow 层 = 子工作流）** | 统一语义，一套可观测，代价是层级变深 |
| C. 两层都用，但 Task 层只存状态、不编排 | 等于放弃 DAG 能力 |

**推荐 B。** 理由：你要的"暂停续跑"在 Workflow 层同样需要（一个工作流跑到第 5 个工具卡住了，要不要能停？）。如果 Workflow 层是进程内执行器，那个进程一崩，工作流的进度就丢了，Task 层的持久化也救不回来。

**代价**：父子工作流嵌套，事件历史翻倍，排查时要在两层之间跳。用统一的 `workflow_id` 命名规范来缓解（见 §5）。

---

### D2 · DAG 定义存哪、怎么锁定版本

这是全篇最重要的一条。

用户会在平台上改图。**如果运行中的实例去读"当前最新版"，你一改图，正在跑的实例就崩了**——它的历史是按旧图写的，重放时按新图走，直接 `NonDeterminismError`。

三种做法：

| 选项 | 机制 | 问题 |
|---|---|---|
| A. 运行时实时读 DB | 每步去查当前定义 | ❌ 改图即崩，且不可复现 |
| B. 启动时快照进工作流输入 | 把整张图 JSON 作为 `run()` 的参数 | ✅ 简单可靠，但输入变大 |
| C. 启动时锁定版本号，运行时按版本读 | 输入里只放 `version`，执行器按版本查库 | ✅ 输入小，但**执行器仍在重放时读 DB** ❌ |

**推荐 B，且必须把 C 当成陷阱**。C 看起来更优雅，但重放时读 DB 是致命的：DB 里那个版本被删了、被改了、或者读到的是缓存——重放就不确定。

正确做法：

```
启动请求
  → 读 task 当前版本 → 反序列化成图
  → 解析每种节点引用（workflow_id + version）
  → 组装成完整可执行的 plan（含所有子图快照）
  → plan 作为工作流输入，一次性写入事件历史（不可变）
  → 之后所有重放都从历史里取 plan
```

代价：输入可能几百 KB（大图），注意 2MB payload 上限（见 §8-场景 7）。

**推论**：图一旦启动就冻结。用户中途改图，只影响**下一次**启动。这正是你要的语义，但要在 UI 上说清楚"本次运行使用 v3 版本"。

---

### D3 · 通用解释器 vs 代码生成

| 选项 | 说明 | 评价 |
|---|---|---|
| A. 每种图生成一段 Python 代码，动态加载 | 灵活、类型好 | ❌ 运行中改图就没法生成；热加载破坏确定性；沙箱限制 |
| B. **通用解释器：图是数据，解释器是固定代码** | 一份代码，所有图 | ✅ 唯一能与"用户随时改图"共存的做法 |
| C. 用 Temporal 的 Dynamic Workflow | 按名字分派 | 解决的是"多工作流类型"，不解决"DAG 是数据" |

**推荐 B。** 代价：解释器本身的 bug 会污染所有流程；拓扑排序、并发控制、失败传播这些逻辑必须写得极其小心，而且**必须确定性**（见 §7）。

解释器的本质是一个小型调度器，代码量不大（200–400 行），但每一步都要审查。

---

### D4 · 暂停/恢复/取消 三态语义

这是产品体验的核心，必须一次设计对。

```
running ──pause──→ paused ──resume──→ running
   │                                      │
   └────────cancel────────→ cancelled ←────┘
   │
   └────────fail──────────→ failed
```

实现：

```python
@workflow.signal
def pause(self) -> None: self._paused = True
@workflow.signal
def resume(self) -> None: self._paused = False
@workflow.signal
def cancel_run(self) -> None: self._cancelled = True
```

解释器主循环里：

```python
while pending:
    # 暂停检查点：只在"准备启动新步骤"之前
    await workflow.wait_condition(lambda: not self._paused or self._cancelled)
    if self._cancelled:
        return {"status": "cancelled", "done": list(done)}
    ready = [...]
    ...
```

**语义定死**：

| 动作 | 正在跑的步骤 | 已完成的步骤 | 之后 |
|---|---|---|---|
| pause | 让它跑完 | 保留 | 不再启动新步骤 |
| resume | — | 保留 | 从下一个未完成步骤继续 |
| cancel | 发取消信号，等它收尾（`WAIT_CANCELLATION_COMPLETED`） | 保留但不推进 | 终态，不可续 |
| terminate | 直接杀 | 保留 | 终态，仅管理员可用，用于卡死 |

`cancel` 要真正生效，Activity 必须配置：

```python
await workflow.execute_activity(
    run_tool, ...,
    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    heartbeat_timeout=timedelta(seconds=30),
)
```

否则取消会"发出去没人理"，用户看到的是按钮点了没反应。

**成本**：暂停中的工作流不占 Worker、不占内存、不消耗资源，只占一条事件历史。停一个月都行。这是 Temporal 相对"轮询式调度"的最大优势。

---

### D5 · 步骤之间用子工作流还是 HTTP 调用

你的旧系统里工作流是"跑在路径下"的，天然会想到 Task 去 POST 那个路径。

| 选项 | 优点 | 缺点 |
|---|---|---|
| A. Task 调 HTTP 路径 | 复用现有契约，解耦彻底 | ❌ 丢失父子关系、丢失统一追踪、幂等要自己做、超时语义割裂 |
| B. **Task 直接 `start_child_workflow`** | 统一事件历史、取消能传播、重试策略统一 | 要求 Task 引擎能定位到工作流定义 |
| C. 混合：内部走子工作流，路径只做对外 facade | 兼得 | 要维护两条入口 |

**推荐 C。** 路径保留，但只是**对外的门**：`POST /flows/order-sync` → 内部 `client.start_workflow(ToolFlow.run, ...)`。Task 内部编排时走子工作流，不走 HTTP。这样：
- 对外契约稳定（路径不变）
- 内部有完整因果链（谁触发了谁，一眼看到）
- 取消/暂停能沿父子关系传播

**注意**：`ParentClosePolicy` 默认是 TERMINATE——父工作流结束（包括被 terminate）会杀掉子工作流。暂停父不会影响子，但**取消父时要显式决定子怎么办**。建议 `REQUEST_CANCEL`，让子自己收尾。

---

### D6 · 元数据和 Temporal 的分工（executions 页的真相）

很多人以为 Temporal 就是执行库，UI 直接查它就行。**不行**，两个硬限制：

1. **可见性有保留期**（dev server 约 1 天，生产可配 1–30 天）。过期就查不到。
2. **可见性不能做复杂业务查询**（没有 join、没有业务字段、分页只有 `next_page_token`）。

所以必须自己维护一张**执行投影表**：

```
Temporal 事件流  ──(订阅/轮询)──→  executions 表（你的 DB）
                                   ├─ 业务字段：创建人、任务名、触发方式
                                   ├─ 状态：running/paused/done/failed
                                   └─ 步骤级进度
```

三种同步方式：

| 方式 | 说明 | 适用 |
|---|---|---|
| A. 工作流内主动写（activity 写库） | 简单，但污染业务逻辑 | 小规模 |
| B. **轮询 `list_workflows` + `describe_workflow`** | 解耦，最终一致 | 推荐起步 |
| C. 消费 Temporal 的 history export / archival | 完整、可回溯 | 长期归档 |

**推荐 B + C**：B 做实时列表，C 做归档（S3），两边都不丢。

另外，步骤级进度别从事件历史里反推（太脆），在解释器里用 `@workflow.query` 暴露 `progress`，控制面定期拉取落库：

```python
@workflow.query
def progress(self) -> dict:
    return {"done": list(self._done), "running": list(self._running)}
```

---

### D7 · 工具执行：归一化的落点

"微服务设计归一化"这句话，落到实处就是**一张 Tool 契约表**：

```python
@dataclass
class ToolSpec:
    id: str
    kind: str              # http | script | llm | builtin
    endpoint: str          # http: URL；script: 代码引用；llm: model
    method: str = "POST"
    input_schema: dict = None
    output_schema: dict = None
    timeout_s: int = 30
    retry: dict = None     # 覆盖默认重试策略
    idempotent: bool = False   # 决定失败后能否安全重试
    secrets: list[str] = None  # 引用的凭据名，不落库明文
```

**四个必须统一的东西**（这才是"归一化"的实质，不是用了同一个框架）：

1. **入参/出参 schema**——否则编排时无法校验，NL 生成也无法约束
2. **错误码分类**——`retryable` / `fatal` / `need_human`，决定重试策略和告警级别
3. **幂等键**——`{run_id}:{step_id}:{attempt_group}`，每个工具必须能接受
4. **超时语义**——`start_to_close` 是单次尝试，`schedule_to_close` 是含重试的总时长

**关键陷阱**：HTTP 工具默认**不幂等**。Temporal 的重试是至少一次，一个"创建订单"的 HTTP 调用重试三次，就是三张订单。所以 `idempotent=False` 的工具必须要求调用方传幂等键，或者禁止自动重试。

---

### D8 · 多租户 / 命名空间

如果只有你一个团队用，先不做。但**表结构里留 `tenant_id` 字段**，成本为零，后期加不用改表。

真要做隔离：Temporal Namespace + 每租户独立 task queue 前缀 + 行级权限。过早做会把 demo 拖死。

---

## 5. 数据模型

```sql
-- 定义层（全部有版本，版本不可变）
tool            (id, tenant_id, name, kind, spec_json, created_by, created_at, updated_at)
workflow        (id, tenant_id, name, path, description, current_version,
                 created_by, created_at, updated_at)
workflow_ver    (id, workflow_id, version, graph_json, input_schema, output_schema,
                 created_by, created_at)            -- 只增不改
task            (id, tenant_id, name, description, current_version,
                 created_by, created_at, updated_at)
task_ver        (id, task_id, version, dag_json, policy_json, created_by, created_at)

-- 运行层（投影，Temporal 是真相源，这里是为了查询和业务字段）
run             (id, tenant_id, kind,          -- 'workflow' | 'task'
                 def_id, def_version,
                 temporal_workflow_id, temporal_run_id,
                 status,                        -- running/paused/done/failed/cancelled
                 trigger,                       -- manual/webhook/schedule/api
                 triggered_by, input_ref, output_ref,
                 started_at, ended_at, error_json)
step_run        (id, run_id, step_id, status, attempt,
                 tool_or_workflow_id, started_at, ended_at, output_ref, error_json)

-- 外围
webhook         (id, tenant_id, path, secret, target_workflow_id, mapping_json, enabled)
audit_log       (id, tenant_id, actor, action, object_type, object_id, ts, detail_json)
idempotency     (key, result_json, created_at)     -- 工具幂等
```

**Temporal 侧命名规范**（非常重要，排查时的救命稻草）：

```
task      : tk:{task_id}:v{ver}:{run_seq}
workflow  : wf:{workflow_id}:v{ver}:{uuid}
child     : tk:{task_id}:v{ver}:{run_seq}:s{step_id}
webhook   : hk:{webhook_id}:{body_hash}       ← 用 REJECT_DUPLICATE 天然去重
```

大对象（工具输出、文件）**一律不写进历史**，写对象存储，history 里只放 key。这是 claim-check 模式，默认 payload 上限 2MB，超了工作流直接失败。

---

## 6. 代码骨架

> 骨架，不是完整实现。省略了错误处理、日志、指标。

### 6.1 工具 Activity（幂等 + 取消感知）

```python
from temporalio import activity

@activity.defn
async def run_tool(tool_id: str, params: dict, idem_key: str) -> dict:
    activity.heartbeat({"phase": "start"})           # 让取消能被感知
    cached = await idem_store.get(idem_key)
    if cached is not None:
        return cached

    spec = await tool_registry.get(tool_id)
    activity.heartbeat({"phase": "call"})
    out = await dispatch(spec, params)               # http / script / llm

    await idem_store.put(idem_key, out)
    return out
```

### 6.2 工作流解释器（含暂停）

```python
import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class ToolFlow:
    def __init__(self) -> None:
        self._paused = False
        self._cancelled = False
        self._done: list[str] = []
        self._running: list[str] = []

    @workflow.signal
    def pause(self) -> None: self._paused = True

    @workflow.signal
    def resume(self) -> None: self._paused = False

    @workflow.signal
    def cancel_run(self) -> None: self._cancelled = True

    @workflow.query
    def progress(self) -> dict:
        return {"done": list(self._done), "running": list(self._running)}

    @workflow.run
    async def run(self, plan: dict) -> dict:
        steps = {s["id"]: s for s in plan["steps"]}   # 启动时快照，之后不变
        done: dict[str, dict] = {}
        pending = dict(steps)

        while pending:
            # 唯一的暂停检查点：只在步骤边界
            await workflow.wait_condition(lambda: not self._paused or self._cancelled)
            if self._cancelled:
                return {"status": "cancelled", "done": list(done)}

            ready = [s for s in pending.values()
                     if all(d in done for d in s.get("needs", []))]
            if not ready:
                return {"status": "deadlock", "remaining": list(pending)}

            # 按固定大小切块，而不是用 Semaphore —— 保证重放顺序确定
            for i in range(0, len(ready), plan.get("concurrency", 8)):
                chunk = ready[i:i + plan["concurrency"]] if plan.get("concurrency") else ready[i:i + 8]
                self._running = [s["id"] for s in chunk]
                results = await asyncio.gather(*[
                    workflow.execute_activity(
                        run_tool,
                        args=[s["tool"], s["params"], f"{plan['run_id']}:{s['id']}"],
                        start_to_close_timeout=timedelta(seconds=s.get("timeout_s", 30)),
                        retry_policy=RetryPolicy(maximum_attempts=3,
                                                 initial_interval=timedelta(seconds=2),
                                                 backoff_coefficient=2.0),
                        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                    ) for s in chunk
                ], return_exceptions=True)
                self._running = []

                for s, r in zip(chunk, results):
                    if isinstance(r, Exception):
                        if s.get("on_fail", "fail") == "continue":
                            done[s["id"]] = {"error": str(r)}
                        else:
                            raise                                   # 整个流程失败
                    else:
                        done[s["id"]] = r
                    pending.pop(s["id"], None)
                    self._done.append(s["id"])

        return {"status": "done", "outputs": done}
```

三个确定性要点，注释里写了两个，第三个是 `dict` 的插入顺序（Python 3.7+ 保证），所以 `pending.values()` 的顺序是稳定的——**前提是 plan 的 JSON 顺序稳定**，所以入库时不要用无序结构。

### 6.3 Task 层（父工作流 + 子工作流 + 暂停）

```python
@workflow.defn
class TaskFlow:
    def __init__(self) -> None:
        self._paused = False
        self._cancelled = False

    @workflow.signal
    def pause(self) -> None: self._paused = True
    @workflow.signal
    def resume(self) -> None: self._paused = False
    @workflow.signal
    def cancel_run(self) -> None: self._cancelled = True

    @workflow.run
    async def run(self, plan: dict) -> dict:
        nodes = {n["id"]: n for n in plan["nodes"]}
        done: dict[str, dict] = {}
        pending = dict(nodes)
        inflight: dict[str, object] = {}

        while pending or inflight:
            await workflow.wait_condition(lambda: not self._paused or self._cancelled)
            if self._cancelled:
                return {"status": "cancelled", "done": list(done)}

            # 启动所有就绪节点（并发，不阻塞）
            if not self._paused:
                for n in [x for x in pending.values()
                          if all(d in done for d in x.get("needs", []))]:
                    inflight[n["id"]] = await workflow.start_child_workflow(
                        ToolFlow.run, n["graph"],
                        id=f"{workflow.info().workflow_id}:s{n['id']}",
                        task_queue="orchestrator",
                        parent_close_policy=ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
                    )
                    pending.pop(n["id"])

            # 等任意一个完成（gather 全部，简单但不最优；生产用 wait(FIRST_COMPLETED)）
            if inflight:
                ids = list(inflight)
                res = await asyncio.gather(*[inflight[i] for i in ids],
                                           return_exceptions=True)
                for i, r in zip(ids, res):
                    inflight.pop(i)
                    if isinstance(r, Exception):
                        raise
                    done[i] = r
        return {"status": "done", "outputs": done}
```

**注意**：`asyncio.wait(FIRST_COMPLETED)` 在 Temporal 里要小心——它返回的集合顺序不确定，重放会炸。上面的 `gather` 全等版本牺牲一点并行度换取确定性，是**正确的起步选择**。

### 6.4 Worker 启动

```python
async def main():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client, task_queue="orchestrator",
        workflows=[ToolFlow, TaskFlow],
        activities=[run_tool],
        max_concurrent_workflow_tasks=100,
        max_concurrent_activities=200,
    )
    await worker.run()
```

---

## 7. 确定性红线（打印出来贴墙上）

Workflow 代码会被**重放**。重放时任何"和上次不一样"的东西，都会让历史与代码对不上，直接 `NonDeterminismError`，流程卡死。

| 禁止 | 替代 |
|---|---|
| `time.time()` / `datetime.now()` | `workflow.now()` |
| `random.random()` | `workflow.random()` |
| `uuid.uuid4()` | `workflow.uuid4()` |
| `os.environ[...]` | 启动时作为输入传入 |
| 遍历 `set` | 用 `list` 或 `sorted()` |
| 依赖对象默认 `hash`（内存地址） | 显式定义 key |
| 直接 HTTP / DB 调用 | 放到 Activity 里 |
| `asyncio.wait(FIRST_COMPLETED)` | `asyncio.gather`，或按固定顺序 |
| 多线程 / `threading` | 单线程 `asyncio` |
| 读取"当前最新版"配置 | 启动时快照进 plan |

**沙箱**：Python SDK 会把 workflow 代码放进受限沙箱，导入 `os`、`socket` 之类会告警或失败。这不是限制你，是保护你——它把"不确定"挡在编译期，而不是等生产环境重放时才发现。

**版本演进**：解释器代码本身要改怎么办？用 `workflow.patched("fix-2026-11")` 分支，旧执行走旧路径、新执行走新路径。或者更省事的做法：**把解释器代码做得极薄，所有可变逻辑都放进 plan 数据**。

---

## 8. 十二个边界场景手册

生产上真正杀人的不是主流程，是这些。

| # | 场景 | 现象 | 对策 |
|---|---|---|---|
| 1 | Worker 跑到一半被 kill -9 | Activity 超时后被重投到别的 Worker，重跑 | 工具必须幂等；否则关自动重试 |
| 2 | 暂停期间发版、Worker 全重启 | 无缝，无需处理 | 无（这正是 Temporal 的价值） |
| 3 | 运行中用户改了任务图 | 旧实例不受影响 | 启动快照（D2），UI 显示"使用 v3" |
| 4 | 上游工具超时 | 默认按 retry_policy 重试，然后失败 | 区分 `start_to_close` 与 `schedule_to_close` |
| 5 | 上层取消，下层子工作流还在跑 | 子工作流被 REQUEST_CANCEL 或孤儿 | 显式设 `ParentClosePolicy` |
| 6 | 一次 fan-out 一万个步骤 | 事件历史爆掉（5 万事件 / 50MB） | 分批 + `continue_as_new` |
| 7 | 工具返回 5MB JSON | payload 超限，工作流直接失败 | claim-check，写对象存储传 key |
| 8 | 两个 Worker 同时处理同一工具 | 不该发生，但 DB 写入可能重复 | 幂等键 + 唯一索引兜底 |
| 9 | 暂停一个月，期间集群迁移 | 恢复时重放很慢（历史长） | 定期 `continue_as_new` 压缩历史 |
| 10 | 时钟：跨天的定时步骤 | 时长不准 | 只用 `workflow.sleep`，不用本地时钟 |
| 11 | 用户删了被引用的工作流版本 | 新启动失败 | 版本不可删，只能归档；引用检查 |
| 12 | Temporal 保留期过了，历史查不到 | executions 页数据丢失 | 自己落投影表 + 归档 S3（D6） |

---

## 9. 可观测性

**必须有的指标**（Prometheus）：

- `workflow_execution_failed_total`（按 workflow type、按失败原因）
- `activity_failed_total`（按 tool_id —— 这是定位"哪个外部服务不稳定"的关键）
- `workflow_task_timeout_total`（重放不确定的早期信号）
- `schedule_to_start_latency`（任务排队等了多久——容量规划）
- `paused_execution_gauge`（暂停堆积，说明有人在手工干预）

**必须有的日志关联键**：`workflow_id` + `run_id` + `step_id` + `attempt`，四个字段贯穿 API 日志、Worker 日志、工具日志。没有这个，排查是一次考古。

**Temporal 自带的 Web UI**（`:8233`）看单个执行的事件时间线很好用，但**不要指望它当管理后台**：没有业务字段、没有 DAG 图、不能按创建人筛。

---

## 10. 对外路径与 Webhook

### 路径 facade

```
POST /flows/{path}
  Header: Authorization: Bearer <token>
  Body:   {"input": {...}}
  → 202 {"execution_id": "...", "status": "running"}

POST /flows/{path}?wait=30s
  → 200 {"output": {...}}        # 最多等 30 秒，超时返回 202
GET  /flows/{path}/executions/{id}
  → {"status": "...", "steps": [...]}
```

**关键设计**：同步等待要用 `?wait=` 显式声明，不要让调用方猜。绝大多数长流程应该走异步 + 回调。

### Webhook

```
POST /hooks/{id}
  Header: X-Signature: hmac-sha256(...)
  Body:   任意（由 mapping 决定）
```

四件事必须做：

1. **签名验证**（HMAC + 时间戳，防重放）
2. **幂等**：`workflow_id = hk:{id}:{sha256(body)}` + `WorkflowIdReusePolicy.REJECT_DUPLICATE` —— 同一个 body 打两次，第二次直接返回第一次的执行 ID。这是免费的去重。
3. **mapping**：用 JSONPath/JMESPath 把外部 payload 映射成工作流输入，别让外部格式渗进内部
4. **限流**：按 webhook id 做令牌桶，防被打爆

**坑**：webhook 要求 200 返回很快（很多平台 3 秒超时）。所以必须"收下就返回 202"，真正干活异步。同步等结果的 webhook 一定会在高峰期超时重投，然后你被重试打爆。

---

## 11. NL → 工作流生成

这是你说的"不那么重要"的部分，但它决定平台的上限感。四种方案：

| 方案 | 做法 | 优点 | 缺点 |
|---|---|---|---|
| A. 一次性生成 JSON | LLM 输出符合 schema 的图 | 简单 | 复杂需求一次成型率低 |
| B. **工具选择 + 增量拼装** | LLM 从工具目录里挑，逐节点生成，边生成边校验 | 成功率高，可解释 | 要写编排循环 |
| C. Agent 式（多轮澄清） | LLM 反问缺什么参数，再生成 | 体验最好 | 交互复杂，要会话状态 |
| D. 微调专用模型 | 用自己的历史工作流训练 | 最贴合业务 | 数据量不够前不要碰 |

**推荐 B，逐步演进到 C。**

关键 guardrail（**没有这些千万别上线**）：

1. **生成 ≠ 执行**。LLM 产出必须经过：schema 校验 → 工具存在性校验 → 类型检查 → DAG 无环检查 → **人工确认** → 存为草稿版本
2. **工具目录要喂给模型**（名称 + 描述 + 入参 schema），别让模型凭空编工具名
3. **禁止模型输出代码**，只输出结构化图。代码执行面太大，且无法审计
4. **生成结果要有 diff**：让用户看到"新增了 3 个节点、改了 1 条边"，而不是一个黑盒图
5. **失败要能退化**：生成不出来就给个模板，别给报错

**顺带一个架构好处**：因为你的图是数据（D2/D3），NL 生成天然适配——生成的就是那份 JSON，和 UI 手拖出来的东西完全同构。如果当初选了"代码生成"路线，这里就得重写。

---

## 12. 先花十分钟评估现成方案

在动手前值得看一眼，避免重复造 UI：

| 产品 | 与你的需求重合度 | 差异 |
|---|---|---|
| **Windmill** | 高：脚本/流程/UI/调度/webhook 全有 | 强在"脚本即流程"，DAG 编排能力弱一些；自托管 Rust 单二进制，很轻 |
| **Kestra** | 高：YAML 声明式 DAG + UI + 触发器 | 声明式，动态改图体验一般；Java 栈 |
| **n8n / Dify** | 中：可视化编排 + webhook | 偏集成/LLM 场景，工程化调度（重试、超时、断点）弱 |
| **Airflow** | 中 | 批处理导向，暂停续跑、事件驱动都不是它的强项 |
| **Prefect / Dagster** | 中 | 数据管道导向 |

**我的判断**：如果你要的是"可控的执行语义 + 自己掌控的元数据模型"，Temporal 是对的底座，UI 和定义管理本来就得自己写。如果你只是要"快点有个能用的平台"，Windmill 值得花两小时试一下——它可能已经覆盖了 60%。

这个判断不影响后面的设计：**即使最后换底座，三层结构、Tool 契约、快照版本、暂停语义这四件事都是通用的。**

---

## 13. 分阶段 Demo 计划

目标不是"跑通"，是**证伪**。每一阶段都配一个"故意破坏"实验。

### P0 · 环境（半天）

```powershell
# 1) Temporal CLI（自带 dev server，单文件，无需 Docker）
winget install Temporal.TemporalCLI
temporal server start-dev --db-filename .\temporal.db --ui-port 8233

# 2) Python 环境
python -m venv .venv-t
.\.venv-t\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install temporalio fastapi uvicorn httpx
```

> ⚠️ **先确认 `temporalio` 有没有 cp314 的 wheel**。你机器上是 Python 3.14.10，这个 SDK 的轮子通常滞后一个大版本。装不上就退回 3.12/3.13 建 venv，别去折腾从源码编译（要 Rust 工具链，很痛苦）。
> 验证：`python -c "import temporalio; print(temporalio.__version__)"`

**验收**：浏览器打开 `http://localhost:8233` 能看到 Temporal UI。

### P1 · 线性工具流 + 崩溃恢复（1 天）

- 写 `run_tool`（先只支持 `sleep` 和 `http`）
- 写 `ToolFlow` 解释器，只支持线性（`needs` 单前驱）
- 客户端启动一次 10 步的流程，每步 sleep 5 秒

**破坏实验**：跑到第 4 步时 `Ctrl+C` 杀 Worker，重启 Worker。
**判据**：第 1–4 步**不重跑**（看 activity 调用日志），第 5 步继续。若重跑了，说明幂等/重放有问题，必须先修。

### P2 · DAG + 并行（1 天）

- 支持 `needs` 多前驱、`concurrency` 分块并发
- 造一个菱形图：A → (B, C) → D

**破坏实验**：把 B 的 timeout 设成 1 秒，工具 sleep 5 秒。
**判据**：B 重试 3 次后失败，D **不执行**，流程状态 = failed，C 正常完成（`return_exceptions=True` 生效）。

### P3 · Task 层 + 暂停续跑（最关键，2 天）

- `TaskFlow` + 子工作流
- API：`start / pause / resume / cancel / status`

**实验 1（核心需求）**：5 步任务，跑到第 2 步后 `pause`，杀 Worker、杀 dev server、重启，再 `resume`。
**判据**：第 3–5 步各执行一次，第 1–2 步**零次**重跑。

**实验 2（快照隔离）**：跑到第 2 步时，在平台上改任务图（加一步、改一条边），保存为新版本。
**判据**：当前实例**完全不受影响**，仍按旧图跑完；新启动的实例才用新图。

**实验 3（暂停成本）**：`pause` 后放 24 小时（或用 `start_time_skipping` 测试环境把时间压缩）。
**判据**：恢复后无缝续跑；期间 Worker 的 CPU 占用为零。

### P4 · API + executions 投影（2 天）

- FastAPI：定义 CRUD、启动/暂停/查询
- 后台任务：轮询 `list_workflows` 落库到 `run` 表
- 一个最简 HTML 页面（甚至不用 React）：三个 tab 对应 Workflows / Executions / Webhooks

**实验**：把 dev server 的 retention 设成 1 小时，跑几十个执行，等过期。
**判据**：Temporal UI 里查不到了，但你的 `run` 表**还在**——证明投影表是必要的。

### P5 · Webhook + 路径 facade（1 天）

- `POST /hooks/{id}` + HMAC + 幂等
- `POST /flows/{path}` facade

**实验**：同一个 body 连发 5 次。
**判据**：只产生 1 个执行，其余 4 次返回同一个 `execution_id`。

### P6 · NL 生成（2 天，可选）

- 先用**假的 LLM**（硬编码几个模板）跑通"生成 → 校验 → 存草稿 → 人工确认"的链路
- 再接真模型，只替换生成那一步

**实验**：让模型生成一个引用了不存在工具的图。
**判据**：被 schema 校验拦住，返回可读错误，**不会**进入执行。

### 时间盒子

保守估计 **8–10 个工作日**做到"能演示"。其中 P3 占一半时间，因为暂停语义的各种边界会吃掉你大量时间。

---

## 14. 验证清单：先证伪，再投入

按"如果不成立，方案就要改"的排序：

| # | 假设 | 实验 | 不成立怎么办 |
|---|---|---|---|
| 1 | 启动快照能让"运行中改图"完全隔离 | P3 实验 2 | 说明 plan 里还有东西在运行时读 DB，逐个揪出来 |
| 2 | 暂停 24 小时成本为零 | P3 实验 3 | 若恢复慢，加 `continue_as_new` 压缩历史 |
| 3 | 子工作流取消能正确传播 | 取消父，看子状态 | 改 `ParentClosePolicy` 或手动 cancel |
| 4 | Temporal 保留期会丢数据 | P4 实验 | 确认必须自建投影（大概率成立） |
| 5 | `temporalio` 在你的 Python 上能装 | P0 | 换 3.12/3.13 venv |
| 6 | 2MB payload 上限够用 | 传 10MB 结果 | claim-check 提前做，别等踩 |
| 7 | 一万步 fan-out 不会炸历史 | 造 1 万节点 | 必须分批 + continue-as-new，P2 就要加 |
| 8 | 开发者的确定性纪律能守住 | Code Review 抽查 | 加 lint 规则 + 沙箱告警转 error |

---

## 15. 风险登记

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| 团队不适应"确定性"约束，写出必崩的 workflow | 高 | 高 | 解释器收敛到 2–3 个文件由专人维护；业务方只写图，不写 workflow 代码 |
| 自建 UI 的工作量被低估 | 高 | 中 | P4 先做最丑的 HTML；或直接嵌 n8n 的编辑器 |
| Temporal 运维复杂度 | 中 | 中 | 生产直接用 Temporal Cloud，或单节点 PG 起步 |
| 工具契约不统一，退化成"每个工具一个特例" | 高 | 高 | D7 的四条统一必须在一开始就落地，后期改成本极高 |
| 暂停语义被业务方误用成"取消" | 中 | 中 | UI 上三个按钮文案必须区别明显 + 二次确认 |
| NL 生成产出不可信的图 | 中 | 中 | guardrail 五条，缺一不上线 |

---

## 16. 开放问题（回来一起定）

1. 工作流的"路径"是全局唯一，还是按租户/项目分区？
2. Task 的"停止"是否需要区分"暂停"和"中止"两个按钮？我建议要。
3. 执行历史保留多久？影响是否要上归档。
4. 工具的脚本执行要不要支持任意语言？只支持 Python 会简单一个量级。
5. 是否需要在工作流内部支持循环（for-each over a list）？这会让解释器复杂度翻倍，但很多场景离不开。
6. 谁能改图、谁能启动、谁能取消——权限模型现在定，比以后加省事。
7. 你现在的旧系统里，工作流和任务的定义是怎么存的？如果是代码，迁移成本要提前算。

---

### 附：一页速查

- **定义**：Tool → Workflow → Task，三层同构，图是数据
- **执行**：Temporal，两层都是 Workflow，父子关系串联
- **地基**：启动快照 + 版本锁定，运行中改图不影响在跑的
- **暂停**：signal + `wait_condition`，零成本，粒度=步骤边界
- **取消**：`WAIT_CANCELLATION_COMPLETED` + `ParentClosePolicy`，否则按钮是假的
- **列表**：必须自建投影表，Temporal 可见性有保留期
- **归一化**：schema + 错误码 + 幂等键 + 超时语义，四条缺一不可
- **生成**：产出图不产出代码，五道 guardrail
- **验证**：P1 崩溃恢复、P3 暂停续跑、P3 快照隔离，这三个过了才算成立
