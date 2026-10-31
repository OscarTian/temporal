# 本机验证全过程记录

> 日期：2026-10-31
> 机器：Windows / `DESKTOP-IAG2QET` / 用户 `admin`
> 目的：用最小可行代码，把 Temporal 能不能扛住 Agentic CAE 平台的四项需求验证一遍。
> 这份文档回答一个问题：**为了得到 `RESULTS.md` 里那几条结论，我到底一步一步干了什么。**

---

## 0. 先看结论在哪

| 想知道什么 | 看哪个文件 |
|---|---|
| 验证出了什么结论 | `RESULTS.md` |
| 换台机器怎么复现 | `RUNBOOK.md` |
| 目标机要装哪三样 | `SETUP.md` |
| 怎么一步步做出来的（本文） | `RUN-LOG.md` |
| workflow / activity 代码 | `flows.py` |
| 起 worker + 8 个场景 | `demo.py` |
| 单独那个"杀 worker"实验 | `s9_start.py` |
| 每次工具调用的原始流水 | `data/ledger.jsonl` |
| 8 个场景的机器可读结果 | `data/report.json` |
| 平台整体设计 | `cae-platform-sync.md`、`temporal-platform-design.md` |

---

## 1. 环境（这些版本号是怎么来的）

```powershell
C:\Program Files\Python314\python.exe -V      # → Python 3.14.10
python -c "import temporalio; print(temporalio.__version__)"   # → 1.35.0
temporal --version                             # → CLI 1.9.1 / Server 1.32.0 / UI 2.54.1
```

要点：
- **没有建虚拟环境**。`temporalio` 装在全局，因为 CPython 3.14 有现成的 `cp314` 轮子，不需要编译。
- **Temporal 服务端不是单独装的**。`temporal server start-dev` 一个命令把 frontend / history / matching / UI 全起在一个进程里，存储用 SQLite。
- 服务端在 `C:\Users\admin\temporal-demo\`，用你原来的窗口跑着：

```powershell
cd C:\Users\admin\temporal-demo
temporal server start-dev --db-filename .\temporal.db --ui-port 8233
```

| 参数 | 作用 |
|---|---|
| `start-dev` | 开发模式：单进程、SQLite、自动建表、UI 一起起 |
| `--db-filename .\temporal.db` | 把数据落成文件。**不加这个就是纯内存，一关全没** |
| `--ui-port 8233` | Web UI 端口 |

端口：gRPC `7233`、UI `8233`、metrics `56575`。

---

## 2. 文件清单

```
cae-demo\
├─ flows.py          151 行   workflow + activity 定义
├─ demo.py           197 行   worker 启动 + 场景驱动
├─ s9_start.py        29 行   只跑"杀 worker"那个实验的客户端
├─ temporal-smoke.py  55 行   冒烟测试（client→server→worker→activity）
├─ dump-history.py    59 行   抓单个执行的全部事件与输入输出
├─ RESULTS.md                结论
├─ RUN-LOG.md                本文
├─ RUNBOOK.md                换机复现手册
├─ SETUP.md                  目标机要装的三样东西
├─ cae-platform-sync.md      平台架构同步（会议 + 专家意见 + 我的分析）
├─ temporal-platform-design.md  Temporal 通用设计备忘
├─ data\
│   ├─ ledger.jsonl          每次工具调用一行（jsonl，可 grep）
│   └─ report.json           8 个场景的 pass/fail
├─ run.log                   8 场景那次的控制台输出
└─ s9.log                    S9 那次的客户端输出
```

**为什么拆成两个 py 文件**——见第 5 节错误 ①。简单说：Temporal 会把 workflow 所在模块**重新导入一遍**做沙箱隔离，模块级只要有 `pathlib.resolve`、`os.environ` 这类东西就直接崩。所以 `flows.py` 里模块级只留常量，所有 I/O 下沉到函数体。

---

## 3. 时间线：每一步做了什么

### 阶段 1 · 确认服务端活着

```powershell
Test-NetConnection localhost -Port 7233 -InformationLevel Quiet
```

输出 `True`（同时有一条 IPv6 `::1` 连接失败的警告，不影响——客户端走 IPv4）。

> 为什么先做这步：我的执行环境**没有外网**（TLS 被沙箱挡了），但能连 `localhost`。所以整条验证链必须全在本机闭环。

---

### 阶段 2 · 写第一版代码（结构错了）

一开始想把所有东西塞进一个 `demo.py`：假求解器、6 端点、workflow、worker、场景。

**结果**：worker 一启动就崩，三个进程全挂。

```
RestrictedWorkflowAccessError: Cannot access pathlib.Path.resolve.__call__
from inside a workflow.
```

原因：模块级写了

```python
ROOT = Path(__file__).resolve().parent     # ← 就是这一行
```

Temporal 起 workflow worker 时，会把 workflow 所在模块**在一个受限沙箱里重新 import 一次**，`Path.resolve()` 被判为不允许的调用。

**修法**：拆成两个文件。
- `flows.py`：只放 workflow / activity 定义，模块级零副作用（常量用字符串拼，不用 pathlib）
- `demo.py`：所有文件路径、argparse、入口，都在这边

---

### 阶段 3 · 起三个 worker

三个独立进程，分别盯三个队列：

```powershell
cd "C:\Users\admin\Documents\deepseek-harness\default-workspace\cae-demo"
& "C:\Program Files\Python314\python.exe" -u demo.py worker --queues orchestrator
```

同样命令跑三次，`--queues` 分别给 `orchestrator`、`host-a`、`host-b`。

| 片段 | 作用 |
|---|---|
| `& "C:\Program Files\Python314\python.exe"` | 显式给全路径。PATH 里没有 `python`，`python3` 更不存在 |
| `-u` | 不缓冲 stdout。**不加这个，后台进程的日志要等进程结束才刷出来** |
| `worker` | `demo.py` 的位置参数，进 worker 分支（第 193 行） |
| `--queues orchestrator` | 只有一个队列；代码里支持逗号分隔多个（第 35 行 `queues[1:]` 那段），演示"一个 worker 监听多个队列"的中心化形态 |

启动成功会打一行：

```
[worker pid=20992] queues=['orchestrator']
```

`orchestrator` 那个进程额外注册了 `ToolFlow`（第 32—33 行），因为只有它需要执行 workflow 代码；`host-a` / `host-b` 只注册 `dispatch` 这个 activity。

---

### 阶段 4 · 第一次跑，卡死 5 分钟

```powershell
python -u demo.py run *> run.log
```

等了 300 秒没有任何输出，被我挪到后台。此时做了三个诊断：

```powershell
# ① 工作流到底有没有在跑
temporal workflow list --limit 20
```

```
Status     WorkflowId    Type
Running    s1-dag        ToolFlow   5 minutes ago      ← 卡在这
Completed  smoke-1       Greet      ...                 ← 之前冒烟测试的
```

```powershell
# ② 工具到底有没有被调用
Get-Content data\ledger.jsonl        # → 文件不存在
```

```powershell
# ③ 谁在报错 —— 直接看 worker 进程的输出
```

worker 的 stderr 里躺着真凶：

```
TypeError: execute_activity() takes from 1 to 2 positional arguments
but 5 positional arguments (and 3 keyword-only arguments) were given
```

> 这里踩了一个**观测坑**：一开始我用 `... | Select-Object -Last 30` 包住输出，结果 `Select-Object -Last` 会等整条流结束才吐，看起来像"完全没输出"。排查后台任务时，**要么直接落文件，要么别接 `-Last`**。

---

### 阶段 5 · 修 `execute_activity` 的参数

Python SDK 的签名是 `execute_activity(fn, arg=None, *, args=[...], ...)`——**只能位置传一个参数**，多参数必须打平成 `args=`。

改前（`flows.py` 第 127 行附近）：

```python
workflow.execute_activity(
    dispatch,
    s.get("tool", "run"),          # ← 第 2 个位置参数就爆了
    s.get("params", {}),
    ...
)
```

改后：

```python
workflow.execute_activity(
    dispatch,
    args=[s.get("tool", "run"),
          s.get("params", {}),
          f"{workflow.info().workflow_id}:{s['id']}",
          plan.get("scenario", "default")],
    task_queue=s.get("queue", "host-a"),
    start_to_close_timeout=timedelta(seconds=s.get("timeout", 30)),
    retry_policy=RetryPolicy(maximum_attempts=s.get("attempts", 2), ...),
)
```

顺手把卡住的那次执行清掉：

```powershell
temporal workflow terminate --workflow-id s1-dag --reason "fix code"
```

| 参数 | 作用 |
|---|---|
| `--workflow-id` | 要终止的业务 ID |
| `--reason` | 写进历史的原因，排查时有用 |

> 为什么必须 terminate：坏掉的 workflow 会**无限重试 workflow task**，worker 日志每秒刷一次堆栈。不清理的话，后面看日志全是噪音。

---

### 阶段 6 · 第二次跑：S6 无限续跑

S1—S5 全过，S6（120 步 + continue-as-new）卡住。

```powershell
temporal workflow describe --workflow-id s6-loop
```

```
Status          Running
HistoryLength   1              ← 每次只有 1 条，说明一直在开新执行
StateTransitionCount  1
```

```powershell
temporal workflow list --limit 10
```

```
Running         s6-loop    now
ContinuedAsNew  s6-loop    1 second ago
ContinuedAsNew  s6-loop    2 seconds ago
... 每 1 秒一条，无限
```

**原因**：我的阈值判断写的是"总完成步数"——

```python
if len(done) >= threshold:          # ← done 里包含了上次执行带过来的 25 步
    workflow.continue_as_new({**plan, "carry_done": done})
```

新执行一上来 `done` 就已经是 25，`25 >= 25` 立刻成立，于是又 continue，永远不干活。

**修法**：记下本次执行开始时的基数，只算新增。

```python
done = dict(plan.get("carry_done", {}))
base = len(done)                    # 本次执行起点
...
if len(self._done) - base >= threshold:      # 只算这次新增的
    workflow.continue_as_new({**plan, "carry_done": done})
```

（`flows.py` 第 110 行、第 118 行）

---

### 阶段 7 · 第三次跑：两个"假 FAIL"

重跑后出现 2 个 FAIL，但都是**测量方式的问题，不是功能问题**：

| 场景 | 报告值 | 真实原因 |
|---|---|---|
| S4 失败重试 | 调用 6 次 | 上一轮的 3 条还在 ledger 里，累加了 |
| S6 循环 | 调用 190 次 / 执行链 65 次 | 同上，且 `s6-loop` 这个 ID 复用了之前被 terminate 的那串执行 |

**修法两条**：
1. 每次跑之前清账本：`LEDGER.unlink(missing_ok=True)`（`demo.py` 第 169 行）
2. 工作流 ID 加时间戳后缀：`self.tag = str(int(time.time()))[-6:]`，`full_id()` 拼接（`demo.py` 第 49、52 行）

改完重跑 → **8/8 全过**。

> 这条值得记：**验证跑失败时，先怀疑测量，再怀疑被测系统。** 我这两次都是自己数错了。

---

### 阶段 8 · S9 杀 worker：第一次没杀成

```powershell
python -u s9_start.py *> s9.log       # 后台启动，8 步 × 0.7 秒
```

然后想按进程名找 worker 杀掉：

```powershell
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like "*--queues host-a*" }
```

```
拒绝访问 (PermissionDenied)
```

沙箱不允许枚举进程。而且等我去查的时候，8×0.7=5.6 秒早跑完了，`describe` 显示 `COMPLETED`。

**修法**（两个都改）：
1. **不用 `Get-CimInstance`**，改用直接杀掉我自己的后台任务（等价于在窗口里按 Ctrl+C）
2. **把步子放慢**：6 步 × 2.0 秒 = 12 秒，留出窗口期

同时换一个全新的 workflow id，避免和上次混淆：

```python
{"params": {"sleep": 2.0}} ... for i in range(6)
id="s9-kill-2"
```

---

### 阶段 9 · S9 第二次：成功

**步骤 1** — 起流程（后台）

```powershell
python -u s9_start.py *> s9.log
```

**步骤 2** — 等 5 秒，确认还在跑

```powershell
temporal workflow describe --workflow-id s9-kill-2
```

**步骤 3** — 杀掉 `host-a` 的 worker 进程（用工具层的 job_kill，杀掉那个后台任务）

**步骤 4** — 等 4 秒，查状态

```powershell
temporal workflow describe --workflow-id s9-kill-2
```

关键片段：

```
Pending Activities: 1
  ActivityId            6
  State                 Started
  Attempt               1
  MaximumAttempts       2
  LastStartedTime       12 seconds ago
  LastHeartbeatTime     12 seconds ago
  LastWorkerIdentity    32440@DESKTOP-IAG2QET      ← 死掉的那个
  LastHeartbeatDetails  [{"idem":"s9-kill-2:k5","tool":"run"}]
```

到这一步已经证明了核心能力：**worker 进程死了，服务端仍然记得"第 6 步正在跑、跑在哪台机器、进度是什么"。**

**步骤 5** — 重启 worker

```powershell
python -u demo.py worker --queues host-a
```

**步骤 6** — 等 45 秒（activity 的 `start_to_close` 超时要先到，服务端才会判定失败并重派）

**步骤 7** — 看结果

```powershell
temporal workflow describe --workflow-id s9-kill-2
```

```
Status    COMPLETED
Result    {"status":"done","steps":6}
```

```powershell
Get-Content data\ledger.jsonl | Where-Object { $_ -match 's9-kill-2' }
```

```
idem                attempt   pid
s9-kill-2:k0            1      32440
s9-kill-2:k1            1      32440
s9-kill-2:k2            1      32440
s9-kill-2:k3            1      32440
s9-kill-2:k4            1      32440
s9-kill-2:k5            1      32440     ← 死在这里
s9-kill-2:k5            2      26356     ← 新 worker 重跑了一次
```

**k0—k4 一次，k5 两次。** 这就是 at-least-once 的精确边界。

---

### 阶段 10 · 清理

```powershell
# 关掉我起的三个 worker（服务端没动，你原来的 dev server 还在跑）
```

验证过程没有杀过 server、没有改过 `temporal.db`、没有清过历史。

---

## 4. 命令速查表

验证过程里用到的所有命令，按用途归类：

### 起服务端

```powershell
temporal server start-dev --db-filename .\temporal.db --ui-port 8233
```

### 起 worker

```powershell
python -u demo.py worker --queues orchestrator     # workflow 执行器
python -u demo.py worker --queues host-a           # 模拟"装了工具 A 的机器"
python -u demo.py worker --queues host-b           # 模拟"装了工具 B 的机器"
python -u demo.py worker --queues orchestrator,host-a   # 一个 worker 盯多个队列
```

### 跑场景

```powershell
python -u demo.py run            # 8 个场景，约 40 秒
python -u s9_start.py            # 单跑"杀 worker"实验
```

### 看工作流

```powershell
temporal workflow list --limit 20
temporal workflow describe --workflow-id s9-kill-2
temporal workflow terminate --workflow-id s1-dag --reason "fix code"
```

| 命令 | 什么时候用 |
|---|---|
| `list` | 看整体在跑什么、卡在哪个 ID |
| `describe` | 看单个执行的详情：状态、待办 activity、**最后心跳来自哪台机器** |
| `terminate` | 强杀。坏掉的 workflow 会无限重试，必须清掉 |

### 看本地记录

```powershell
Get-Content data\report.json -Encoding UTF8
Get-Content data\ledger.jsonl -Encoding UTF8 |
  ForEach-Object { $_ | ConvertFrom-Json } | Format-Table
Get-Content run.log -Encoding UTF8
```

> `-Encoding UTF8` 必须加，否则中文全是乱码（PowerShell 默认按 ANSI 读）。

---

## 5. 四个真错误

| # | 报错 / 现象 | 根本原因 | 修法 | 教训 |
|---|---|---|---|---|
| ① | `RestrictedWorkflowAccessError: pathlib.Path.resolve` | workflow 模块会在沙箱里被重新 import，模块级不能有非确定性调用 | 拆出 `flows.py`，模块级零副作用 | **workflow 文件要当"纯函数"写** |
| ② | `TypeError: execute_activity() takes from 1 to 2 positional arguments` | SDK 只接受一个位置参数，多参数要打平成 `args=` | 改成 `args=[...]` | 卡住时先看 worker 的 stderr，不要只盯客户端 |
| ③ | S6 无限 `ContinuedAsNew` | 阈值按"总步数"算，continue 之后立刻又满足 | 记 `base`，只算本次新增 | **阈值类逻辑，永远要问"重置之后还成立吗"** |
| ④ | 两个假 FAIL | 账本不清空 + workflow ID 复用 | `unlink` + ID 加时间戳 | 验证失败先怀疑测量 |

另外两个**环境限制**（不是代码错）：

| 现象 | 原因 | 绕法 |
|---|---|---|
| `Get-CimInstance` 拒绝访问 | DSH 沙箱禁止枚举进程 | 直接杀掉我自己起的后台任务 |
| `curl: (35) schannel SEC_E_NO_CREDENTIALS` | 沙箱挡 TLS 凭据，外网全断 | 只能在本机跑，或你在自己终端跑 |

---

## 6. 每个场景对应代码哪里

`demo.py`：

| 场景 | 函数 | 行 | 判据 |
|---|---|---|---|
| S1 DAG 编排 | `s1_dag` | 76 | 顺序必须是 `s1 → s2/s3 → s4` |
| S2 按主机分发 | `s2_routing` | 84 | 每步的 `task_queue` 与预期队列一致 |
| S3 暂停/恢复 | `s3_pause` | 99 | 暂停期间 ledger 新增 == 0，且 query 返回 `paused=True` |
| S4 失败重试 | `s4_retry` | 115 | 调用 3 次，attempt = 1/2/3 |
| S5 超时 | `s5_timeout` | 126 | `start_to_close=1s` 而任务 5s → 失败 |
| S6 循环 | `s6_loop` | 135 | 120 次调用 / 执行链 > 1 次 |
| S7 改图 | `s7_snapshot` | 147 | 旧实例 4 步、新实例 5 步 |
| S8 payload | `s8_payload` | 158 | 3MB 返回值必须失败 |
| 调度总入口 | `all` | 168 | 清账本 → 依次跑 → 写 `report.json` |

`flows.py`：

| 关键点 | 行 | 说明 |
|---|---|---|
| `dispatch` | 33 | 6 端点的统一入口，记录 pid / 队列 / attempt |
| `ToolFlow` | 75 | 通用解释器 |
| `pause` / `resume` / `stop` | 83 / 87 / 91 | 三个 signal |
| `progress` | 95 | query，前端轮询进度用 |
| `continue_as_new` | 118 | 历史保护 |
| `execute_activity` | 128 | **`task_queue` 是变量，这是按主机分发的地方** |

`task_queue` 那一行是整个验证里最重要的一行代码：

```python
task_queue=s.get("queue", "host-a"),
```

DSL 里每个节点带一个 `queue` 字段，运行时直接决定任务派到哪台机器。**这就是"按仿真工具分发"的答案。**

---

## 7. 复现步骤

```powershell
# ── 窗口 1：服务端（一直开着）
cd C:\Users\admin\temporal-demo
temporal server start-dev --db-filename .\temporal.db --ui-port 8233

# ── 窗口 2/3/4：三个 worker
cd C:\Users\admin\Documents\deepseek-harness\default-workspace\cae-demo
python demo.py worker --queues orchestrator
python demo.py worker --queues host-a
python demo.py worker --queues host-b

# ── 窗口 5：跑 8 个场景（约 40 秒）
cd C:\Users\admin\Documents\deepseek-harness\default-workspace\cae-demo
python demo.py run

# ── 窗口 6：S9，单独跑
python s9_start.py
# 看到 host-a 窗口在动之后，直接 Ctrl+C 杀掉它
# 等 30 秒，再把 host-a 那个窗口重新起起来
# 回到窗口 6 看结果，然后对比 data\ledger.jsonl
```

关键的观察点，按顺序：

1. `data\report.json` 里 8 条 `ok: true`
2. `temporal workflow describe --workflow-id s9-kill-2` 在杀 worker 之后仍然 `Running`，且 `Pending Activities` 里有 `LastWorkerIdentity` 是死掉那台
3. 重启 worker 后变 `COMPLETED`，而 `ledger.jsonl` 里只有正在飞的那一步出现两次

---

## 8. 结论出自哪次运行

| 结论 | 证据来源 |
|---|---|
| 已有步骤不重跑 | 阶段 9，`ledger.jsonl` 里 k0—k4 各 1 条 |
| 飞行中的步骤会重试 | 阶段 9，k5 有 attempt 1 和 2 |
| 分发是运行时决定的 | 阶段 7，S2 两个队列各自命中，2 个进程 |
| 循环要配 continue-as-new | 阶段 6（失败）+ 阶段 7（成功，4 次执行链） |
| payload 上限 2MB | 阶段 7，S8 的 3MB 直接失败 |
| 暂停是干净的 | 阶段 7，S3 暂停期间新增 0 |
| 一次最简执行 ≈ 11 条事件 | 早期冒烟测试 `temporal-smoke.py`，`dump-history.py` 读数 |

---

## 9. 没验证的，别当成已验证

| 项 | 原因 | 真要验证需要 |
|---|---|---|
| server 重启后恢复 | 会打断你正在跑的 dev server | 你同意后压测一次 |
| 跨机器分发 | 单机多进程已能证明分发逻辑 | 两台机器 + 网络可达 |
| Elasticsearch 可见性 | dev server 没有 | docker compose 全量部署 |
| 高并发吞吐 | 单进程 server，数据无参考价值 | 全量部署 + 压测 |
| 归档 / 保留期 | 同上 | 全量部署 |
| 数据 > 2MB 的正确做法 | 只验证了"会失败" | 得真的接一个 S3 |
