"""Workflow + Activity 定义。

注意：这个模块会被 Temporal 的 workflow 沙箱重新导入，
所以 **模块级代码必须是确定性的**：不能有 pathlib.resolve、os.environ、
time.time() 之类。所有 I/O 一律放进 activity 函数体里。
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

LEDGER = os.path.join("data", "ledger.jsonl")


def _log(rec: dict) -> None:
    rec.setdefault("scenario", "default")
    rec["ts"] = round(time.time(), 3)
    os.makedirs("data", exist_ok=True)
    with open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# --------------------------------------------------------------------------
# 6 端点的假实现：真实世界里这里是一次 HTTP 调用或一次 HPC 提交
# --------------------------------------------------------------------------
@activity.defn
async def dispatch(tool: str, params: dict, idem: str, scenario: str = "default") -> dict:
    info = activity.info()
    _log(
        {
            "scenario": scenario,
            "pid": os.getpid(),
            "queue": info.task_queue,
            "activity_id": info.activity_id,
            "attempt": info.attempt,
            "workflow_id": info.workflow_id,
            "tool": tool,
            "idem": idem,
        }
    )
    activity.heartbeat({"tool": tool, "idem": idem})

    if tool == "prepare":
        await asyncio.sleep(0.05)
        return {"ok": True, "tool": tool, "idem": idem}
    if tool == "run":
        await asyncio.sleep(params.get("sleep", 0.05))
        return {"job_id": f"job-{idem}", "submitted": True}
    if tool == "status":
        await asyncio.sleep(0.02)
        return {"pct": 100, "done": True}
    if tool == "collect":
        await asyncio.sleep(0.02)
        return {"artifact": f"s3://cae/{idem}.dat"}
    if tool == "slow":
        await asyncio.sleep(params.get("sleep", 5))
        return {"ok": True}
    if tool == "boom":
        raise RuntimeError(f"boom at {idem}")
    if tool == "big":
        return {"payload": "x" * int(params.get("size", 3 * 1024 * 1024))}
    return {"ok": True, "tool": tool}


# --------------------------------------------------------------------------
# 编排层：把 DSL 当数据的通用解释器（DAG + 暂停 + continue-as-new）
# --------------------------------------------------------------------------
@workflow.defn
class ToolFlow:
    def __init__(self) -> None:
        self._paused = False
        self._cancelled = False
        self._done: list[str] = []
        self._running: list[str] = []

    @workflow.signal
    def pause(self) -> None:
        self._paused = True

    @workflow.signal
    def resume(self) -> None:
        self._paused = False

    @workflow.signal
    def stop(self) -> None:
        self._cancelled = True

    @workflow.query
    def progress(self) -> dict:
        return {
            "done": list(self._done),
            "running": list(self._running),
            "paused": self._paused,
        }

    @workflow.run
    async def run(self, plan: dict) -> dict:
        done: dict[str, dict] = dict(plan.get("carry_done", {}))
        base = len(done)          # 本次执行开始时已有多少步（continue-as-new 带过来的）
        self._done = list(done)
        pending = {s["id"]: s for s in plan["steps"] if s["id"] not in done}
        threshold = plan.get("max_steps_per_run", 10**9)

        while pending:
            # 唯一的暂停检查点：步骤边界
            await workflow.wait_condition(lambda: not self._paused or self._cancelled)
            if self._cancelled:
                return {"status": "cancelled", "done": list(done)}

            # 历史保护：本次执行新增步数超过阈值，就结束本次、开新一次，状态带过去
            if len(self._done) - base >= threshold:
                workflow.continue_as_new({**plan, "carry_done": done})

            ready = [s for s in pending.values()
                     if all(d in done for d in s.get("needs", []))]
            if not ready:
                return {"status": "deadlock", "remaining": list(pending)}

            chunk = ready[: plan.get("concurrency", 4)]
            self._running = [s["id"] for s in chunk]
            results = await asyncio.gather(*[
                workflow.execute_activity(
                    dispatch,
                    args=[s.get("tool", "run"),
                          s.get("params", {}),
                          f"{workflow.info().workflow_id}:{s['id']}",
                          plan.get("scenario", "default")],
                    task_queue=s.get("queue", "host-a"),
                    start_to_close_timeout=timedelta(seconds=s.get("timeout", 30)),
                    retry_policy=RetryPolicy(
                        maximum_attempts=s.get("attempts", 2),
                        initial_interval=timedelta(milliseconds=200),
                        backoff_coefficient=2.0,
                    ),
                )
                for s in chunk
            ], return_exceptions=True)
            self._running = []

            for step, res in zip(chunk, results):
                sid = step["id"]
                pending.pop(sid, None)
                if isinstance(res, BaseException):
                    if step.get("on_fail") == "continue":
                        done[sid] = {"error": str(res)}
                    else:
                        raise res
                else:
                    done[sid] = res
                self._done.append(sid)

        return {"status": "done", "steps": len(done)}
