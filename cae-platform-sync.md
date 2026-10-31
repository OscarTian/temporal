# Agentic CAE 平台 · 架构信息同步

> 版本 v0.4 · 2026-10-31
> 用途：会议信息 + 专家意见 + 我的分析 + 实测结果，持续维护。
> 配套：`temporal-platform-design.md`（Temporal 通用设计备忘，讲原理和做法）
> 实测细节：`RESULTS.md`（结论）、`RUN-LOG.md`（全过程）
>
> **标注约定**
> 【输入】= 会议/专家原话，我只做归并
> 【推断】= 我的分析，可能错，需要你判断
> 【待确认】= 需要你或专家回答
> 【已证】= 我已经在本机跑出来或查到的事实

---

## 0. 一句话需求

> 一个平台，把原来「流程设计引擎 + 微服务管理平台」两件事全干了；额外要求流程引擎能按自然语言描述自动搭工具流（待定是否强需求）。

**要回答的核心问题**：Temporal 能覆盖到哪，哪些必须我们自己开发。

---

## 1. 术语对齐（先说清楚，否则后面全歪）

同一个东西，会议上有三种叫法。建议以后统一：

| 我的旧词 | 会议用词 | 实际指 |
|---|---|---|
| Tool（工具） | **工具** | 装了求解器的软件，如 Ansys/Abaqus |
| Tool 封装体 | **微服务** | 一个工具按 6 端点封装后的可调用单元 |
| Workflow | **工作流 / 流程** | 微服务编排成的图 |
| Task / Run | **任务** | 选微服务开一次运行，可启停续 |
| 能力目录 | **能力目录（三级）** | 手册 / 契约 / 工作流 API |

**关键定调【输入】**：*"每个工具可以封装成一个微服务，工作流中进行编排的原子部件是微服务。"*
→ 也就是说，**编排的原子是微服务，不是工具**。这条决定了后面所有设计。

---

## 2. 会议原始信息（归并后）

### 2.1 定位与分层

- C4 架构 L2 容器层，**控制面**里包含 Temporal 工作流引擎，负责调度、持久化、恢复。
- 讲解者的比喻：**像 Airflow，但支持循环**。
- 出现了一个没听懂的词：**"耦合调度"**【待确认 Q2】。

### 2.2 工具微服务的 6 个标准动作【输入】

所有仿真工具微服务（内联算子 / 轻量脚本 / 重型求解器）**统一暴露 6 个标准动作**：

| # | 动作 | 职责 |
|---|---|---|
| 1 | `prepare` | 参数校验、网格提取、生成输入求解脚本 |
| 2 | `run` | 提交作业至本地/VM/HPC，启动异步状态机 |
| 3 | `status` | 正则解析日志流，提取百分比 / 迭代步 |
| 4 | `control` | cancel / pause / resume / checkpoint |
| 5 | `collect` | 归一化输出为 Artifact，写入 Lineage |
| 6 | `describe` | 返回 capabilities/*.yaml 契约描述 |

### 2.3 两条用户线

**A. 平台用户线（消费侧）**
选工程模组 → 从数据平台选数据 → 编排工作流（或直接用模板）→ 智能装箱调度 → license 校验 → 执行
→ sidecar 拦截 + PEP 鉴权 → 驱动统一生命周期（编排/控制/运行）→ 运行中收敛判定、闭环迭代
→ 看结果 → 产物沉淀为不可篡改 lineage → 工作流发布为**一条龙只读 API**

**B. 开发运维线（供应侧）**
云主机装求解器 → 制作版本化**黄金镜像 AMI** → 登记平台 → 生成工具手册
→ 运维登记基本信息 + 多模态手册 URI → 按 6 端点写实现（**或让 agent 读手册自动抽取输入输出与生命周期契约**）
→ 自动化测试 + schema 校验打桩 → 人工审批 → 契约冻结 + 服务目录注册
→ 绑定 Host Profile、纳管 → 闲时缩容到 0 但**固定 ENI**（MAC 绑定）

### 2.4 资源面板【输入】

- 容量管理 / Host Profile / 装箱调度面板
- 实时算例与显存监控（CAE Telemetry）：CPU 核心占用、RAM、GPU 计算负荷、VRAM
- **Host Profile 主机画像**：按 hostprofile 名显示，含系统、CPU 核总数/占用、内存总数/占用、已装工具、运行并发策略（`install: shared / runtime: concurrent`）、license 绑定（`server-token 余量 2/4`）、状态 `READY` / `BUSY` / `SCALED_TO_ZERO`
- 装箱调度：选工具 → 输入资源需求 → 选主机 → 执行

### 2.5 编排画布【输入】

- 可拖拽画布，节点配置输入输出，本质上就是 6 端点的图形化
- **画布的事实来源是 DSL**（画布只是 DSL 的编辑器）

### 2.6 专家意见 A（懂 Temporal）【输入】

- Temporal 还缺的：**容器系统**（必须我们接入）、**数据库/外部系统接入**、**能力目录**（工具手册 / 服务契约 / 工作流三级视图）
- 机制描述：scheduler 规划 → 任务进消息队列 → worker 拉取执行。*"定义好微服务，塞给他他就帮你执行"*
- **Temporal 与 sidecar 是解耦的**
- 底层执行平面 = worker：worker 监控队列；可以一台机器一个队列一个 worker，也可以**一个 worker 监控多个队列、多台机器**（中心化部署）
- sidecar 负责文件上传 S3 统一接口等；**sidecar 优先级低**
- 能力目录三层：
  1. **工具手册** —— 注册工具时产生，装箱时靠它把工具装上去
  2. **服务契约** —— 有了契约，agent 可把工具封装成微服务并发布，此时**外部已可独立调用**（*"也是走 temporal，只不过单节点（单微服务）"*）
  3. **工作流 API** —— 一条龙契约

### 2.7 专家意见 B（懂 Airflow，不懂 Temporal）【输入】

- Airflow 里**看不到执行机有哪些能力/工具**；它是中心化的，通过 Celery 挂若干 worker，默认认为这些 worker **等价**
- Celery 可以注册，把一组机器注册成一组 worker
- Scheduler 解析 DAG 拓扑：1 → (2,3) 时先跑 1，再并行 2、3；单机就顺序执行
- 多机 = Redis + Celery，worker 等价 → 1/2/3 随机分配
- **但他的疑问正是我们的需求**：机器上装了不同工具，任务依赖特定工具，就需要**按工具定向分配**
- Airflow 的做法：配置 **Celery worker 队列**，声明某类 worker 支持某类任务
- **他不知道 Temporal 里怎么做这件事**

### 2.8 下达的验证需求【输入】

1. Temporal 部署 + **分布式部署（任务队列）**
2. 基于 Temporal 编排工作流
3. 工具微服务在 Temporal 框架下**如何开发**
4. 调度时**根据任务分发到对应仿真工具**

---

## 3. 我的分层整理【推断】

```
控制面
├─ 编排画布（DSL 编辑器）
├─ 能力目录（三级）
├─ 装箱调度器 + license 管理
├─ Temporal（持久化执行内核）
└─ 平台 API（对外只读 API / 任务启停）

执行面
├─ worker（拉队列、跑 activity）   ← 可按主机/工具划分队列
└─ 6 端点工具微服务（sidecar 或宿主进程）

资源面
├─ Host Profile（画像 + 策略）
├─ 黄金镜像 AMI / 容器镜像
└─ ENI 固定 / 缩容到 0

数据面
├─ Artifact（S3）
├─ Lineage（不可篡改）
└─ CAE Telemetry
```

---

## 4. Temporal 能力边界表【推断 + 已证】

这张表是本文档的核心，直接回答"哪些要自己做"。

| 能力 | Temporal 提供？ | 说明 / 对策 |
|---|---|---|
| 持久化执行、崩溃恢复 | ✅ 原生 | 事件历史 + 重放，这是它的看家本事 |
| 任务按类型分发到指定 worker | ✅ **task queue** | 见 §5.1，这是回答专家 B 的关键 |
| 重试 / 超时 / 心跳 | ✅ 原生 | per-activity 配置 |
| 循环 / 迭代收敛 | ✅ 代码即控制流 | ⚠️ 但必须配 `continue-as-new`，否则历史爆 |
| 定时 / cron 触发 | ✅ Schedules | |
| 可视化 DAG 编辑 | ❌ | 自建画布，DSL 为源 |
| 实时日志流、日志回溯 | ❌ | Temporal 只存事件，不存 stdout。自己做（见 §5.5） |
| 容器编排 / 启停机器 | ❌ | 接 K8s / 云 API，见 §5.4 |
| 外部数据库 / 外部系统接入 | ❌ | 写进 activity，自己管连接与事务 |
| 能力目录（手册/契约/API） | ❌ | 自建，见 §6 |
| 按业务字段查执行列表 | ⚠️ 需 ES | 生产自建投影表；dev 用 SQLite 够 |
| 多租户 / 细粒度鉴权 | ⚠️ 有插件但不完整 | 自建 RBAC，PEP 在 sidecar |
| 暂停 / 恢复 | ⚠️ 实验性 | `temporal workflow pause` 标注 Experimental【已证】；建议自写 signal |
| 单任务的实时状态展示 | ✅ Query | `@workflow.query` 暴露进度 |

---

## 5. 关键接缝（最容易出事的地方）【推断】

### 5.1 「按工具/主机分发任务」——Temporal 怎么解专家 B 的疑问

**答案：task queue + worker 监听关系。**

| 部署形态 | 做法 | 适用 |
|---|---|---|
| 等价 worker 池 | 一个队列，N 个 worker 抢 | 无工具依赖的通用步骤 |
| 按工具分池 | `tools-ansys` / `tools-abaqus` 各一个队列，对应 worker 只监听自己的 | 工具装在固定几台机 |
| **按主机分池** | `host-gpu-07` 专属队列，该主机上只跑一个监听它的 worker | 求解器与 license 绑定到机器 |
| 中心化 | 一个 worker 监听多个队列 | 轻量任务、省进程 |

**和 Airflow 的本质差别（重要）**：
Airflow 的 `queue=` 是**任务定义时写死**的静态路由；Temporal 的 `task_queue=` 是**调用时传的参数**，可以按运行时的数据决定。

```python
# 队列名可以是数据算出来的
await workflow.execute_activity(run_tool, job, task_queue=f"host-{host_id}")
```

→ 对"每次任务可能落到不同主机"的场景，**Temporal 比 Airflow 更强**，不是更弱。
代价：队列数量 ≈ 主机数量，需要一套队列命名与生命周期管理。

**待验证**：一台主机一个队列时，worker 心跳、队列可见性、扩缩容怎么管【Q9】。

### 5.2 `run` 端点不能做成阻塞的 activity

求解器跑几小时，一个 activity 阻塞等它，会撞 `start_to_close` 超时、失败重试、甚至重复提交作业。

**正确形态**：
```
activity A: prepare → 提交作业 → 立刻返回 job_id
activity B: 轮询 status（或用 heartbeat）→ 完成
activity C: collect
```
或者一个 activity 内部循环 + `heartbeat()` 报进度，把 `heartbeat_timeout` 当看门狗。

**这是整个设计最关键的一个分叉**，做错了会表现为"求解器跑了两遍"。

### 5.3 `control` 与 Temporal 的 cancel 语义冲突

求解器的 `pause/checkpoint` 是**它自己的状态机**；Temporal 的 cancel 只能取消 activity，它不知道求解器里有没有 checkpoint。

两套生命周期必须显式对齐，否则：
- 用户点"暂停" → 实际把进程杀了
- 恢复时没有 checkpoint → 只能从头算

**建议**：`control` 做成一个独立的 activity/signal，调用求解器自己的控制接口；Temporal 的 cancel 只用于"真不要了"。

### 5.4 容器系统 / 外部系统接入：Temporal 没有

- Temporal 不管机器、不管容器。worker 是你要部署的进程。
- 缩容到 0 + 固定 ENI → 纯云资源层，与 Temporal 无关。
- **冲突点**：冷启动要几分钟，`schedule_to_start_timeout` 必须留够，否则任务在等开机时就超时失败。

### 5.5 日志：Temporal 不提供

Temporal 的"事件历史"不是日志。你在 UI 上看到的不是 stdout。

- 求解器日志 → 写文件 / S3 / ELK，自己做
- 进度百分比 → `activity.heartbeat({"pct": 42})`，能带小 JSON，带不了日志流
- 前端"实时日志" → 另开一条通道（SSE/WebSocket）读日志存储，**不走 Temporal**

### 5.6 循环与历史爆炸

"Temporal 比 Airflow 强在支持循环"——对，但：

- 每次循环迭代至少产生几条事件，历史上限 5 万条 / 50MB
- 闭环迭代（收敛判定）几百轮就会到顶
- **必须配 `continue_as_new`**：跑到阈值就结束当前执行、开一个新的，状态作为输入传下去

这条**几乎没人讲**，但它是这类平台最早爆的雷。

---

## 6. 能力目录三级视图 → 与 Temporal 的映射【推断】

| 层 | 内容 | 谁产生 | Temporal 参与吗 |
|---|---|---|---|
| L1 工具手册 | 安装说明、多模态手册 URI、Host Profile 绑定 | 运维登记 / agent 抽取 | ❌ 完全在平台侧 |
| L2 服务契约 | 6 端点 + capabilities/*.yaml + schema | agent 封装 + 人工审批冻结 | ⚠️ 契约是平台数据；单微服务调用可走 Temporal（短执行） |
| L3 工作流 API | 一条龙只读 API、输入输出契约 | 编排器发布 | ✅ 由 Temporal 执行，平台做 facade |

**注意 L2 的那句"也是走 temporal，只不过单节点"**：
如果每个单微服务调用都起一个 workflow，开销 = 每次至少 3 条事件 + 一次调度往返。
【推断】更合理的做法：单微服务调用走 **activity 直接调用**或短 workflow，只有需要幂等/长跑/可追溯时才起 workflow。这个要实测【Q3】。

---

## 7. 验证计划（对应会议下达的 4 项）

| # | 需求 | 实验设计 | 判据 |
|---|---|---|---|
| V1 | 部署 + 分布式 | dev server 起步；再起 2 个 worker 分别监听 2 个队列 | 任务按队列分流，`DescribeTaskQueue` 能看到 poller |
| V2 | 编排工作流 | DSL(JSON) → 解释器 workflow，跑一个 5 步 DAG | 拓扑顺序正确、并行分支真并行 |
| V3 | 工具微服务开发 | 把一个假求解器包成 6 端点，注册成一个 activity 集合 | prepare→run→status→collect 全链路通 |
| V4 | 按工具分发 | 起 `tools-a` / `tools-b` 两个队列，各一个 worker | 指定工具的步骤只落到对应 worker |

**附加验证（我认为比上面四项更能决定成败）**

| # | 项 | 为什么重要 |
|---|---|---|
| V5 | 中断恢复 | 暂停 → 杀 worker → 重启 → 继续，步骤不重跑 |
| V6 | 运行中改 DSL | 快照隔离，老实例不受影响 |
| V7 | 循环 1000 轮 | 验证历史是否会爆，测 continue-as-new |
| V8 | 冷启动超时 | 模拟机器 5 分钟才起来 |

> **执行情况（2026-10-31）**：V1—V7 已在本机跑通（V7 用 120 步代替 1000 轮），V8 冷启动**未测**。
> 结果见 §8，过程见 `RUN-LOG.md`。

---

## 8. 已证实的事实【已证】

### 8.1 环境与基本链路

在本机（Windows，2026-10-31）实测：

| 项 | 结果 |
|---|---|
| Temporal CLI | 1.9.1（Server 1.32.0，UI 2.54.1） |
| 部署形态 | `temporal server start-dev --db-filename temporal.db`，SQLite |
| Python SDK | `temporalio 1.35.0`，Python 3.14.10，有 cp314 轮子 |
| 冒烟链路 | client → server → worker → activity → 历史，通 |
| 事件数 | 一次最简执行 = 11 条事件，耗时 1.13 秒 |
| 暂停能力 | `temporal workflow pause/unpause` 存在，但标注 **Experimental** |

工具脚本：`temporal-smoke.py`、`dump-history.py`（抓 describe + history）。

### 8.2 九个场景的实测结果

代码在 `demo.py`（worker + 场景）+ `flows.py`（workflow/activity），跑在 3 个独立 worker 进程上（`orchestrator` / `host-a` / `host-b`）。

| # | 场景 | 结果 | 证据 |
|---|---|---|---|
| S1 | DAG 编排（1→(2,3)→4） | ✅ | 顺序 `s1,s2,s3,s4` |
| S2 | 按主机/工具分发 | ✅ | 4 步分别落到 host-a / host-b，涉 2 个进程 |
| S3 | 暂停/恢复 | ✅ | 暂停期间新增调用 **0** |
| S4 | 失败自动重试 | ✅ | attempt 1/2/3 |
| S5 | 超时中断 | ✅ | `start_to_close=1s` 而任务 5s → 失败 |
| S6 | 120 步 + continue-as-new | ✅ | 120 次调用 / 4 次执行链 |
| S7 | 运行中改图 | ✅ | 旧实例 4 步、新实例 5 步 |
| S8 | 大 payload | ✅（预期失败） | 3MB 返回值直接失败（上限 2MB） |
| S9 | 杀 worker 后恢复 | ✅ | 见 8.3 |

### 8.3 中断续跑的精确边界（最重要的一条）

6 步串行，每步 2 秒，第 6 步执行中杀掉 `host-a` worker，4 秒后重启：

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

**已完成的步骤零重跑，只有"正在飞"的那一个被重试一次。**

→ 所以 6 端点的每个动作**必须幂等**。这不是建议，是硬要求。`run` 提交作业尤其危险：重试一次 = 提交两个作业。

### 8.4 两个真实约束（写代码时一定会撞）

> workflow 沙箱禁了 `pathlib.resolve` 和 `os.environ`，只能在 module 级写纯确定性代码；`execute_activity` 多参数必须走 `args=[...]`。

具体表现：

| 约束 | 报错 | 正确做法 |
|---|---|---|
| workflow 模块级不能有非确定性调用 | `RestrictedWorkflowAccessError: Cannot access pathlib.Path.resolve` | workflow 定义单独一个文件，模块级只留常量，所有 I/O 下沉到 activity 函数体 |
| `execute_activity` 只接受一个位置参数 | `TypeError: execute_activity() takes from 1 to 2 positional arguments but 5 were given` | `execute_activity(fn, args=[a, b, c], ...)` |

### 8.5 复现

worker 进程已关闭，dev server 未动。要自己复现：

- `RESULTS.md` 最后一节 —— 完整命令
- `RUN-LOG.md` —— 逐步过程、每条命令的作用、四个错误的来龙去脉

---

## 9. 我的判断【推断】

1. **Temporal 覆盖"调度 + 持久化 + 恢复"这三件事，覆盖得很好**；不覆盖"资源、容器、目录、日志、鉴权"——这些本来就是平台该干的。
2. **"按工具分发"不是 Temporal 的短板，是它的强项**（运行时动态 task queue），比 Airflow 的静态 queue 灵活。专家 B 的疑问可以正面回答。
3. **最大的技术风险不是 Temporal，是"6 端点 → activity"的映射**：`run` 的异步化、`control` 的语义对齐、大产物的 claim-check。这三处做错，整个链路会以"重复执行"或"卡死"的形式失败。
4. **能力目录是纯自建**，而且它是三条用户线的交汇点——L1 手册装机、L2 契约封装、L3 API 发布，本质是同一份元数据的三次投影。
5. **IBM 那条存疑**：1.32.0 是很新的版本，与"3、4 月停止维护"矛盾。建议核实 release 节奏【Q1】。

---

## 10. 待确认问题

| # | 问题 | 影响 |
|---|---|---|
| Q1 | Temporal 维护主体与路线（IBM 说法核实） | 选型根本 |
| Q2 | "耦合调度"具体指什么 | 控制面边界 |
| Q3 | 单微服务调用是否必须走 Temporal | 性能与复杂度 |
| Q4 | sidecar 优先级低 → 文件上传先由谁做 | 数据面落地 |
| Q5 | 画布 DSL 的 schema 是否已定稿 | 解释器与画布能否并行开发 |
| Q6 | license 余量由谁校验（装箱前 or 运行时） | 调度器设计 |
| Q7 | 闭环迭代的轮次上限与 continue-as-new 策略 | 会不会炸历史 |
| Q8 | 多租户 / 工程模组隔离粒度 | 命名空间与权限模型 |
| Q9 | 一主机一队列时，队列生命周期谁管 | 运维复杂度 |
| Q10 | "停止"是 pause 还是 cancel，与 checkpoint 怎么对齐 | 用户体验与数据安全 |
| Q11 | HPC（Slurm）提交是否也走 worker | 架构分支 |

---

## 11. 变更记录

| 版本 | 日期 | 变化 |
|---|---|---|
| v0.1 | 2026-10-30 | 通用 Temporal 设计备忘（另见 design.md） |
| v0.3 | 2026-10-31 | 并入会议原始信息、两位专家意见、能力边界表、验证计划 |
| v0.4 | 2026-10-31 | 补 §8 实测结果：9 个场景、中断续跑边界、两个代码约束；配套 RESULTS.md / RUN-LOG.md |
