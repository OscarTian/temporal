# 换机器前要装的三样东西

> 目标：一台干净的 Windows，装完这三样，就能照 `RUNBOOK.md` 跑通全部验证。
> 全程约 10 分钟，磁盘占用约 200 MB。

---

## 一段话概括（可直接发给 agent）

```
请在目标 Windows 机器上完成三件事，每件都做自检：

1) Python：安装 Python 3.9 以上（python.org 官方安装包，安装时勾选
   "Add python.exe to PATH"）。自检：python --version 能打印版本号。

2) temporalio：执行 python -m pip install temporalio。自检：
   python -c "import temporalio; print(temporalio.__version__)" 能打印版本号。

3) Temporal CLI：从 https://github.com/temporalio/cli/releases 下载
   temporal_cli_*_windows_amd64.tar.gz，解压得到 temporal.exe，放进 PATH。
   自检：temporal --version 输出含 server 版本号。

注意三点：
- temporalio 是 Python 库，Temporal CLI 是服务端可执行文件，两者完全独立，
  缺一不可，版本也不需要一致。
- Windows 上不存在 python3 命令，一律用 python；也不要装完不重启终端就测试。
- 三样齐了之后，进入 cae-demo 目录，执行
  temporal server start-dev --db-filename .\temporal.db --ui-port 8233
  确认 7233 端口在监听、http://localhost:8233 能打开，然后按 RUNBOOK.md 继续。
```

---

## 为什么是这三样

```
┌─────────────────────────────────────────────┐
│  temporal.exe  ← 第 3 样：服务端（含 Web UI） │
│  存状态、派任务、重放历史                     │
└──────────────────┬──────────────────────────┘
                   │ gRPC :7233
┌──────────────────┴──────────────────────────┐
│  Python 进程     ← 第 1 样：解释器            │
│  里的 temporalio  ← 第 2 样：SDK             │
│  运行 workflow 代码、执行 activity            │
└─────────────────────────────────────────────┘
```

**最容易搞混的一点**：Temporal 有两个完全独立的东西，同名不同物。

| 叫法 | 是什么 | 怎么装 | 里面有什么 |
|---|---|---|---|
| Temporal CLI | 一个可执行文件 `temporal.exe`（152 MB） | 下载压缩包解压 | **服务端** + Web UI + 命令行工具 |
| temporalio | 一个 Python 包 | `pip install` | **只有客户端和 worker 的库**，不含服务端 |

只装 pip 那个 → 连不上任何东西（没有服务端）。
只装 exe 那个 → `python demo.py` 直接报 `No module named 'temporalio'`。

两个都要。版本不需要对齐：实测 CLI 1.9.1（内含 server 1.32.0）+ `temporalio` 1.35.0 可以正常工作。

---

## 第 1 样 · Python

### 为什么

`demo.py`、`flows.py`、`s9_start.py` 全是 Python 代码，没解释器就什么都跑不了。

### 装什么

Python **3.9 或以上**。实测用的是 **3.14.10**。

- 官方安装包：<https://www.python.org/downloads/windows/>
- 安装界面**必须勾选** `Add python.exe to PATH`（在第一个页面的底部）

### 自检

```powershell
python --version
```

期望：

```
Python 3.14.10
```

### 装错了会怎样

| 现象 | 原因 | 怎么办 |
|---|---|---|
| `python : 无法将"python"项识别为 cmdlet` | 没勾 PATH | 重装并勾选，或手动加 PATH |
| `python` 打开了微软商店 | Windows 的 0 字节占位别名 | 设置 → 应用 → 高级应用设置 → 应用执行别名，关掉 `python.exe` / `python3.exe` |
| `python3` 找不到 | **Windows 上根本没有这个命令** | 一律用 `python` |

---

## 第 2 样 · temporalio（Python SDK）

### 为什么

Temporal 的 Python SDK。**worker 和 client 都靠它**：连服务端、注册 workflow/activity、发信号、查进度，全在这一个包里。

它**不包含服务端**。装完它，你还是没有可以连接的服务。

### 装什么

```powershell
python -m pip install temporalio
```

如果提示权限不足（全局 Python 装在 `C:\Program Files` 下）：

```powershell
python -m pip install --user temporalio
```

### 自检

```powershell
python -c "import temporalio; print(temporalio.__version__)"
```

期望打印一个版本号，实测 `1.35.0`。

### 装错了会怎样

| 现象 | 原因 | 怎么办 |
|---|---|---|
| `ModuleNotFoundError: No module named 'temporalio'` | 没装，或装到了另一个 Python 解释器 | 用 `python -m pip install` 而不是裸 `pip`，保证装进同一个解释器 |
| `PermissionError` / `Access is denied` | 全局目录没写权限 | 加 `--user`，或用管理员终端 |
| 装了但 import 失败、报编译错误 | 该 Python 版本没有预编译轮子 | 换 3.11—3.14 这类有官方轮子的版本 |

> `python -m pip` 和 `pip` 的区别：前者明确"装给当前这个 python"，后者可能指向别的解释器。**永远用前者。**

---

## 第 3 样 · Temporal CLI（服务端）

### 为什么

这就是服务端本体。`temporal.exe` 一个文件里打包了 frontend / history / matching / worker 四个角色、Web UI，以及命令行工具本身。

`temporal server start-dev` 一条命令，就在一个进程里把整套服务端跑起来，用 SQLite 存数据。**开发验证不需要 Docker、不需要数据库。**

### 装什么（三选一）

**方式 A · 直接拷（离线最快）**

源机器上已经有：

```
C:\Users\admin\AppData\Local\Programs\temporal\temporal.exe
```

拷到目标机，放进任意一个 PATH 目录，或者直接放在 `cae-demo` 目录下（那样命令要写 `.\temporal.exe`）。

**方式 B · 下载解压**

<https://github.com/temporalio/cli/releases> → 找 `temporal_cli_*_windows_amd64.tar.gz` → 解压出 `temporal.exe` → 放进 PATH。

**方式 C · 包管理器**

```powershell
scoop install temporal
```

### 自检

```powershell
temporal --version
```

期望类似：

```
temporal version 1.9.1 (server 1.32.0) (ui 2.54.1)
```

看到三个版本号（CLI / server / UI）就对了。**1.9 以上都能跑本文的验证。**

### 装完再验一次服务端真的能起

```powershell
cd D:\cae-demo
temporal server start-dev --db-filename .\temporal.db --ui-port 8233
```

期望日志里出现：

```
Temporal server is running on 127.0.0.1:7233
Web UI is available at http://127.0.0.1:8233
```

然后浏览器打开 <http://localhost:8233>，能看到界面就成功了。**先 Ctrl+C 停掉**，等正式验证时再起。

### 装错了会怎样

| 现象 | 原因 | 怎么办 |
|---|---|---|
| `'temporal' 不是内部或外部命令` | 没在 PATH | 用全路径，或把 exe 所在目录加进 PATH 后**重开终端** |
| `bind: address already in use` | 7233 或 8233 被占 | 见下 |
| 首次启动很慢 / 被杀软拦 | `temporal.exe` 152 MB，扫描耗时 | 加白名单，或等一会 |

端口被占时：

```powershell
Get-NetTCPConnection -LocalPort 7233 -State Listen | Select-Object OwningProcess
Stop-Process -Id <查到的PID> -Force
```

> **不要改端口**。`demo.py` 里把 `localhost:7233` 写死了，改端口要改代码。直接腾出 7233 更省事。

---

## 一次装完，一条命令自检

把下面四行贴进 PowerShell，全部有输出就算齐了：

```powershell
python --version
python -c "import temporalio; print('temporalio', temporalio.__version__)"
temporal --version
python -c "print('python 和 temporalio 是同一个解释器')"
```

期望四行都有内容，没有 `不是内部或外部命令`、没有 `ModuleNotFoundError`。

---

## 三样都装好之后

```powershell
cd D:\cae-demo
```

然后按 `RUNBOOK.md` 第 3 节开始：起服务端 → 三个 worker → 跑验证。

---

## 离线场景补充

目标机不能联网时，除 `temporal.exe` 外，Python 包也要带过去。

源机器（能联网的）上下载轮子：

```powershell
python -m pip download temporalio -d .\wheels
```

把 `wheels\` 一起拷过去，目标机装：

```powershell
python -m pip install --no-index --find-links .\wheels temporalio
```

> 轮子和 Python 版本、位数绑定。**两边 Python 版本尽量一致**，否则会挑不到合适的包。
