# 打包复现手册 · 换一台机器跑通全部验证

> 目标：把 `cae-demo` 整个目录拷到另一台电脑，照着本文一条条敲，得到和 `RESULTS.md` 一样的结果。
> 全程 5 个命令行窗口，约 10 分钟。

---

## 先回答一个问题：server 是怎么跑起来的？

**不需要任何 py 文件。**

Temporal 的服务端是一个独立的可执行文件 `temporal.exe`（约 152 MB，里面打包了 Server + Web UI）。装好 CLI 之后，一条命令就能把整套服务端起起来：

```powershell
temporal server start-dev
```

这条命令会在**一个进程里**起 frontend、history、matching、worker 四个角色，外加 Web UI 和一个 SQLite 数据库。开发验证够用，生产才需要拆开部署。

Python 这边只负责两件事：**写 workflow/activity 代码**、**起 worker 去拉任务**。它连的是 `localhost:7233`，跟 server 是两个东西。

```
temporal.exe  ← 服务端，独立进程，只管存状态、派任务
     ↑ gRPC :7233
demo.py       ← Python 进程，运行 workflow 代码 + 执行 activity
```

---

## 0. 一页速览

| 窗口 | 干什么 | 命令 |
|---|---|---|
| 1 | 服务端 | `temporal server start-dev --db-filename .\temporal.db --ui-port 8233` |
| 2 | 编排 worker | `python demo.py worker --queues orchestrator` |
| 3 | 主机 A worker | `python demo.py worker --queues host-a` |
| 4 | 主机 B worker | `python demo.py worker --queues host-b` |
| 5 | 跑验证 | `python demo.py run` |
| 5 | 再跑一次（杀 worker） | `python s9_start.py` |

窗口 1—4 要一直开着，别关。

---

## 1. 目标机准备（3 件事）

> 这一节的详细展开（为什么装、装错了什么样、离线怎么办）见 `SETUP.md`。
> 如果你打算让 agent 去做，`SETUP.md` 里有一段可以直接粘过去的说明。

### 1.1 Python

要求 3.9 以上，实测用的是 3.14.10。

```powershell
python --version
```

如果提示找不到 `python`，试 `py --version`；再不行就去 python.org 装一个，勾选 **Add Python to PATH**。

> Windows 上 `python3` 通常不存在，别用那个名字。

### 1.2 装 Python SDK

```powershell
python -m pip install temporalio
```

验证：

```powershell
python -c "import temporalio; print(temporalio.__version__)"
```

期望打印一个版本号（实测 `1.35.0`）。

### 1.3 装 Temporal CLI（就是那个 server）

**这是整件事唯一的"外部依赖"，必须单独装。**

三种方式，任选：

**方式 A：直接拷（最快，适合离线）**
源机器上已经有：

```
C:\Users\admin\AppData\Local\Programs\temporal\temporal.exe
```

把这个文件拷到目标机的任意目录，然后二选一：
- 放进 PATH 里，后面命令写 `temporal`
- 直接放在 `cae-demo` 目录下，后面命令写 `.\temporal.exe`

**方式 B：下载**
<https://github.com/temporalio/cli/releases> 下载 `temporal_cli_*_windows_amd64.tar.gz`，解压得到 `temporal.exe`。

**方式 C：包管理器**
```powershell
scoop install temporal
```

验证：

```powershell
temporal --version
```

期望看到类似：

```
temporal version 1.9.1 (server 1.32.0) (ui 2.54.1)
```

> 版本不用和这里完全一致，1.9 以上都能跑本文的验证。

### 1.4 三项一起自检

```powershell
python --version
python -c "import temporalio; print('sdk', temporalio.__version__)"
temporal --version
```

三条都出东西，就可以往下走了。

---

## 2. 解包与目录核对

### 2.1 解压

把 `cae-demo` 整个目录放到任意位置，比如 `D:\cae-demo`。

### 2.2 核对文件

```powershell
cd D:\cae-demo
Get-ChildItem -File | Select-Object Name
```

应该看到这 11 个文件：

```
cae-platform-sync.md          平台架构同步（会议 + 专家意见 + 分析）
temporal-platform-design.md   Temporal 通用设计备忘
RUN-LOG.md                    本机验证全过程
RESULTS.md                    验证结论
SETUP.md                      要装的三样东西
RUNBOOK.md                    本文
demo.py                       worker 启动 + 8 个场景
flows.py                      workflow / activity 定义
s9_start.py                   "杀 worker"实验的客户端
temporal-smoke.py             冒烟测试（可选）
dump-history.py               抓事件历史（可选）
```

缺文件的话，验证会以 `ModuleNotFoundError: flows` 之类的形式失败。

### 2.3 清掉旧数据（重要）

目录里可能带着我跑出来的旧记录，不清会影响计数：

```powershell
Remove-Item .\data -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item .\temporal.db* -Force -ErrorAction SilentlyContinue
Remove-Item .\run.log,.\s9.log -Force -ErrorAction SilentlyContinue
```

| 删的什么 | 为什么 |
|---|---|
| `data\` | 里面的 `ledger.jsonl` 是上一轮的调用流水，会污染"调用了几次"的判断 |
| `temporal.db*` | 旧的历史库，留着会让 `s9-kill-2` 这类 ID 撞车 |
| `*.log` | 上一轮的控制台输出 |

> 不删也能跑（`demo.py run` 会自己清账本），但 S9 依赖干净的历史库，建议还是删。

---

## 3. 启动服务端（窗口 1）

```powershell
cd D:\cae-demo
temporal server start-dev --db-filename .\temporal.db --ui-port 8233
```

**成功标志**：滚出一堆日志，其中有一行类似

```
Temporal server is running on 127.0.0.1:7233
Web UI is available at http://127.0.0.1:8233
```

**参数解释**：

| 参数 | 作用 | 不加会怎样 |
|---|---|---|
| `start-dev` | 开发模式：单进程 + SQLite + 自动建表 | 这个子命令就是要用这个 |
| `--db-filename .\temporal.db` | 数据落成文件 | **不加就是纯内存，一 Ctrl+C 全部丢失** |
| `--ui-port 8233` | Web UI 端口 | 用默认 8233，没差别 |

**顺手打开 UI**：浏览器访问 <http://localhost:8233>，能打开就说明服务端活了。后面所有验证都可以在 UI 上肉眼看一遍。

**如果 7233 被占用**（比如目标机上已经有 Temporal 在跑）：

```powershell
temporal server start-dev --db-filename .\temporal.db --port 7323 --ui-port 8233
```

但注意：`demo.py` 里硬编码连 `localhost:7233`，换端口的话要改 `demo.py` 第 28 行和 `flows.py` 里的连接。**建议直接杀掉占用的进程**：

```powershell
Get-NetTCPConnection -LocalPort 7233 -State Listen | Select-Object OwningProcess
Stop-Process -Id <上面查到的PID> -Force
```

---

## 4. 启动三个 worker（窗口 2 / 3 / 4）

三个窗口分别执行，**顺序无所谓**，但都要在服务端起来之后再开。

**窗口 2：编排 worker**（唯一一个需要加载 workflow 代码的）

```powershell
cd D:\cae-demo
python demo.py worker --queues orchestrator
```

**窗口 3：主机 A worker**（模拟"装了工具 A 的机器"）

```powershell
cd D:\cae-demo
python demo.py worker --queues host-a
```

**窗口 4：主机 B worker**（模拟"装了工具 B 的机器"）

```powershell
cd D:\cae-demo
python demo.py worker --queues host-b
```

**成功标志**：每个窗口打印一行

```
[worker pid=12345] queues=['orchestrator']
```

然后就不动了——这是正常的，它在等任务。

**参数解释**：

| 片段 | 作用 |
|---|---|
| `python demo.py worker` | 进 worker 分支（`demo.py` 第 193 行） |
| `--queues orchestrator` | 这个 worker 盯哪个队列。**队列名就是分发依据**，写错名字任务永远没人接 |
| 逗号分隔 | `--queues orchestrator,host-a` 可以让一个 worker 同时盯多个队列 |

> 为什么必须三个：`orchestrator` 跑 workflow 逻辑，`host-a` / `host-b` 跑具体工具调用。这是"任务按工具/主机分发"的最小演示。

---

## 5. 跑 8 个场景（窗口 5）

```powershell
cd D:\cae-demo
python demo.py run
```

**耗时约 40 秒**，期间第四列 worker 窗口会刷任务。

**期望输出**（中文在部分终端会乱码，能看懂 `PASS` 就行）：

```
[PASS] S1 DAG 编排: 顺序=['s1', 's2', 's3', 's4']
[PASS] S2 按工具/主机分发: 队列映射正确=True, 进程数=2, pid=[...]
[PASS] S3 暂停/恢复: 暂停期间新增=0(应0), 总调用=6, {'status': 'done', 'steps': 6}
[PASS] S4 失败自动重试: 调用=3次, attempt=[1, 2, 3], 结果=WorkflowFailureError
[PASS] S5 超时中断: 结果=WorkflowFailureError
[PASS] S6 循环+continue-as-new: 120步/调用120次/执行链4次/{'status': 'done', 'steps': 120}
[PASS] S7 改图快照隔离: 旧实例=4步 新实例=5步
[PASS] S8 大 payload 上限: 3MB 返回值 → WorkflowFailureError

=== 8/8 通过 ===
```

**中文乱码怎么办**：

```powershell
chcp 65001
```
或者在 PowerShell 里改用：

```powershell
Get-Content .\data\report.json -Encoding UTF8
```
`report.json` 里是同样的结论，UTF-8 存的，不会有乱码。

**注意**：S4 / S5 / S8 是**故意失败**的场景，日志里出现红色异常是预期行为，不代表验证挂了。看最后一行 `=== 8/8 通过 ===` 为准。

---

## 6. 跑 S9：杀掉 worker 还能续跑（最关键的实验）

这个实验需要你在**特定时刻**手动杀掉一个进程，所以分成几步。

### 步骤 1 · 启动流程（窗口 5）

```powershell
cd D:\cae-demo
python s9_start.py
```

这个脚本会启动一个 6 步、每步 2 秒的串行流程，然后**卡在那里等结果**，正常要 12 秒才结束。

### 步骤 2 · 确认它在跑（另开一个窗口，或先做准备工作）

```powershell
cd D:\cae-demo
temporal workflow describe --workflow-id s9-kill-2
```

看到 `Status  Running` 就对了。

### 步骤 3 · 杀掉窗口 3（host-a worker）

**在 `s9_start.py` 启动后约 5 秒**，切到窗口 3，按 `Ctrl+C`。

> 时机不重要，只要在 12 秒内杀掉就行。杀掉后窗口 5 那边不会有任何反应——这正是要验证的点。

### 步骤 4 · 看服务端记不记得（新窗口）

```powershell
cd D:\cae-demo
temporal workflow describe --workflow-id s9-kill-2
```

期望看到：

```
Status          Running
Pending Activities: 1

  ActivityId            6
  State                 Started
  Attempt               1
  LastWorkerIdentity    12345@YOUR-PC        ← 死掉的那个 worker
  LastHeartbeatDetails  [{"idem":"s9-kill-2:k5","tool":"run"}]
```

**这一步是整个验证的核心**：worker 进程已经死了，服务端仍然完整记得"第 6 步正在跑、跑在哪台机器、进度到哪"。

### 步骤 5 · 把窗口 3 重新起起来

```powershell
cd D:\cae-demo
python demo.py worker --queues host-a
```

### 步骤 6 · 等 45 秒

为什么是 45 秒：activity 的 `start_to_close_timeout` 是 30 秒，服务端要等它超时才会判定"这个 worker 死了，重派别处"。

### 步骤 7 · 看结果

窗口 5 应该已经打印：

```
RESULT: {'status': 'done', 'steps': 6}
```

再核对调用流水：

```powershell
cd D:\cae-demo
Get-Content .\data\ledger.jsonl -Encoding UTF8 |
  Where-Object { $_ -match 's9-kill-2' } |
  ForEach-Object { $_ | ConvertFrom-Json } |
  Select-Object idem, attempt, pid | Format-Table
```

期望看到：

```
idem                attempt   pid
s9-kill-2:k0            1      12345
s9-kill-2:k1            1      12345
s9-kill-2:k2            1      12345
s9-kill-2:k3            1      12345
s9-kill-2:k4            1      12345
s9-kill-2:k5            1      12345     ← 杀在这里
s9-kill-2:k5            2      67890     ← 新 worker 重跑了一次
```

**k0—k4 各一次，k5 两次。** 这就是结论：已完成的步骤不重跑，飞行中的那一步会重试。

---

## 7. 验收：三句话

跑完之后，只要这三条成立，就说明本机复现成功：

1. 窗口 5 最后一行是 `=== 8/8 通过 ===`
2. 杀 worker 之后，`describe` 里仍然能看到 `Pending Activities` 和 `LastWorkerIdentity`
3. `ledger.jsonl` 里 `k5` 出现两次、`k0`—`k4` 各一次

---

## 8. 可选：冒烟测试与抓历史

### 冒烟测试（30 秒确认链路通）

```powershell
cd D:\cae-demo
python temporal-smoke.py
```

期望：`RESULT: hello, WORLD`。

这一个脚本包含了 client、server、worker、activity 全链路，**换机后建议先跑它**，它比 `demo.py` 简单得多，出问题容易定位。

### 抓事件历史（理解 Temporal 存了什么）

```powershell
python dump-history.py --workflow-id smoke-1
```

会打印出这个执行的全部事件（期望 11 条），以及输入输出。

```powershell
python dump-history.py --workflow-id s9-kill-2
```

能直接看到那次杀 worker 的轨迹，重点看最后几步：第 6 个 activity 在 `24.465` 被 `SCHEDULED`，下一次 `STARTED` 已经是 `55.488`——中间这 31 秒，就是 worker 死掉、服务端干等超时的那段时间，之后才重派给新 worker。

> 注意：超时在历史里**不一定有一条独立的 `ACTIVITY_TASK_TIMED_OUT`**。实际观察到的表现是"同一个 activity 的 `SCHEDULED` 之后隔了很久才出现 `STARTED`"，别看错。

---

## 9. 排错速查

| 现象 | 原因 | 怎么修 |
|---|---|---|
| `ModuleNotFoundError: No module named 'flows'` | 没在 `cae-demo` 目录里跑 | `cd` 到脚本所在目录再执行 |
| `ModuleNotFoundError: No module named 'temporalio'` | 没装 SDK | `python -m pip install temporalio` |
| 报连接失败 / `Connection refused` | 服务端没起，或不在 7233 | 回窗口 1 看有没有 `Temporal server is running` |
| `'temporal' 不是内部或外部命令` | CLI 没在 PATH | 用全路径 `.\temporal.exe`，或把 exe 放进 PATH |
| workflow 一直 `Running`，worker 窗口没动静 | 队列名对不上 | 检查 `--queues` 和 DSL 里 `queue` 字段是否一致 |
| worker 一启动就 `RestrictedWorkflowAccessError` | workflow 文件里有 `pathlib` / `os.environ` 之类的模块级代码 | 见 `RUN-LOG.md` 错误 ①，I/O 必须放进 activity |
| 中文全是乱码 | 终端编码 | `chcp 65001`，或直接读 `data\report.json` |
| 后台跑看不到输出 | `Select-Object -Last` 会等流结束 | 别接 `-Last`，或改成 `*> file.log` 再看文件 |
| `s9-kill-2` 说已存在 | 旧的历史库没删 | 删 `temporal.db*` 重来，或换个 workflow id |

---

## 10. 收尾

验证完要清理：

```powershell
# 停掉窗口 2/3/4 的 worker：各自 Ctrl+C
# 停掉窗口 1 的服务端：Ctrl+C（数据已落进 temporal.db，下次还在）
```

想彻底重来：

```powershell
Remove-Item .\temporal.db* -Force
Remove-Item .\data -Recurse -Force
```

---

## 11. 离线场景（目标机没有外网）

需要两样东西能拷过去：

1. **`temporal.exe`**（152 MB）—— 见 1.3 方式 A
2. **`temporalio` 的 wheel**

wheel 在源机器上下载：

```powershell
python -m pip download temporalio -d .\wheels
```

把 `wheels\` 目录一起拷过去，目标机执行：

```powershell
python -m pip install --no-index --find-links .\wheels temporalio
```

> 注意 wheel 和 Python 版本绑定。源机器 3.14 的 wheel 不能装在目标机的 3.10 上，**两边 Python 版本尽量一致**。
