"""宿主脚本：启动 worker / 跑验证场景。

    python demo.py worker --queues orchestrator
    python demo.py worker --queues host-a
    python demo.py run
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from flows import ToolFlow, dispatch

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
LEDGER = DATA / "ledger.jsonl"


# ---------------------------------------------------------------- worker
async def run_worker(queues: list[str]) -> None:
    os.chdir(ROOT)  # 让 flows 里的相对路径一致
    client = await Client.connect("localhost:7233")
    primary = queues[0]
    kwargs = {"task_queue": primary, "activities": [dispatch],
              "max_concurrent_activities": 20}
    if "orchestrator" in queues:
        kwargs["workflows"] = [ToolFlow]
    workers = [Worker(client, **kwargs)]
    for q in queues[1:]:
        workers.append(Worker(client, task_queue=q, activities=[dispatch]))
    print(f"[worker pid={os.getpid()}] queues={queues}", flush=True)
    await asyncio.gather(*[w.run() for w in workers])


# ---------------------------------------------------------------- scenarios
def step(i: str, tool="run", needs=(), queue="host-a", **kw) -> dict:
    return {"id": i, "tool": tool, "needs": list(needs), "queue": queue, **kw}


class Runner:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.tag = str(int(time.time()))[-6:]   # 每次跑用一个独立 tag，避免历史执行干扰计数
        self.results: list[dict] = []

    def full_id(self, wid: str) -> str:
        return f"{wid}-{self.tag}"

    def ledger(self, scenario: str) -> list[dict]:
        if not LEDGER.exists():
            return []
        out = []
        for line in LEDGER.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                if r.get("scenario") == scenario:
                    out.append(r)
        return out

    def record(self, name: str, ok: bool, detail: str) -> None:
        self.results.append({"scenario": name, "ok": ok, "detail": detail})
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)

    async def start(self, wid: str, plan: dict):
        plan = {**plan, "scenario": wid.split("-")[0]}
        return await self.client.start_workflow(
            ToolFlow.run, plan, id=self.full_id(wid), task_queue="orchestrator"
        )

    async def s1_dag(self):
        plan = {"steps": [step("s1", "prepare"), step("s2", "run", ["s1"]),
                          step("s3", "run", ["s1"]), step("s4", "collect", ["s2", "s3"])]}
        out = await (await self.start("s1-dag", plan)).result()
        order = [r["idem"].split(":")[1] for r in self.ledger("s1")]
        ok = out["status"] == "done" and order[0] == "s1" and order[-1] == "s4"
        self.record("S1 DAG 编排", ok, f"顺序={order}")

    async def s2_routing(self):
        plan = {"steps": [step("a1", queue="host-a"), step("b1", queue="host-b"),
                          step("a2", "collect", ["a1"], queue="host-a"),
                          step("b2", "collect", ["b1"], queue="host-b")],
                "concurrency": 4}
        await (await self.start("s2-routing", plan)).result()
        rows = self.ledger("s2")
        by = {}
        for r in rows:
            by.setdefault(r["idem"].split(":")[1], set()).add((r["queue"], r["pid"]))
        ok = all(({q for q, _ in v} == {("host-a" if k.startswith("a") else "host-b")})
                 for k, v in by.items())
        pids = sorted({p for v in by.values() for _, p in v})
        self.record("S2 按工具/主机分发", ok, f"队列映射正确={ok}, 进程数={len(pids)}, pid={pids}")

    async def s3_pause(self):
        plan = {"steps": [step(f"p{i}", "run", params={"sleep": 0.4}) for i in range(6)],
                "concurrency": 1}
        h = await self.start("s3-pause", plan)
        await asyncio.sleep(1.0)
        await h.signal(ToolFlow.pause)
        before = len(self.ledger("s3"))
        prog = await h.query(ToolFlow.progress)
        await asyncio.sleep(2.0)
        during = len(self.ledger("s3"))
        await h.signal(ToolFlow.resume)
        out = await h.result()
        ok = during == before and out["status"] == "done" and prog["paused"] is True
        self.record("S3 暂停/恢复", ok,
                    f"暂停期间新增={during - before}(应0), 总调用={len(self.ledger('s3'))}, {out}")

    async def s4_retry(self):
        plan = {"steps": [step("r1", "boom", attempts=3, timeout=10)]}
        err = None
        try:
            await (await self.start("s4-retry", plan)).result()
        except Exception as e:  # noqa: BLE001
            err = type(e).__name__
        rows = self.ledger("s4")
        ok = err is not None and len(rows) == 3
        self.record("S4 失败自动重试", ok, f"调用={len(rows)}次, attempt={sorted({r['attempt'] for r in rows})}, 结果={err}")

    async def s5_timeout(self):
        plan = {"steps": [step("t1", "slow", params={"sleep": 5}, timeout=1, attempts=1)]}
        err = None
        try:
            await (await self.start("s5-timeout", plan)).result()
        except Exception as e:  # noqa: BLE001
            err = type(e).__name__
        self.record("S5 超时中断", err is not None, f"结果={err}")

    async def s6_loop(self):
        n = 120
        plan = {"steps": [step(f"L{i:03d}", "status") for i in range(n)],
                "concurrency": 10, "max_steps_per_run": 25}
        out = await (await self.start("s6-loop", plan)).result()
        runs = [w async for w in self.client.list_workflows(
            f'WorkflowId = "{self.full_id("s6-loop")}"')]
        rows = self.ledger("s6")
        ok = out["status"] == "done" and len(runs) > 1 and len(rows) == n
        self.record("S6 循环+continue-as-new", ok,
                    f"{n}步/调用{len(rows)}次/执行链{len(runs)}次/{out}")

    async def s7_snapshot(self):
        v1 = {"steps": [step(f"v1-{i}") for i in range(4)], "concurrency": 1}
        h1 = await self.start("s7-snapshot", v1)
        await asyncio.sleep(0.3)
        v2 = {"steps": [step(f"v1-{i}") for i in range(4)] + [step("v2-extra")],
              "concurrency": 1}
        h2 = await self.start("s7-snapshot-v2", v2)
        o1, o2 = await h1.result(), await h2.result()
        ok = o1["steps"] == 4 and o2["steps"] == 5
        self.record("S7 改图快照隔离", ok, f"旧实例={o1['steps']}步 新实例={o2['steps']}步")

    async def s8_payload(self):
        plan = {"steps": [step("big1", "big", params={"size": 3 * 1024 * 1024},
                             attempts=1, timeout=20)]}
        err = None
        try:
            await (await self.start("s8-payload", plan)).result()
        except Exception as e:  # noqa: BLE001
            err = type(e).__name__
        self.record("S8 大 payload 上限", err is not None, f"3MB 返回值 → {err}")

    async def all(self):
        LEDGER.unlink(missing_ok=True)      # 每次跑从干净账本开始
        for fn in (self.s1_dag, self.s2_routing, self.s3_pause, self.s4_retry,
                   self.s5_timeout, self.s6_loop, self.s7_snapshot, self.s8_payload):
            try:
                await fn()
            except Exception as e:  # noqa: BLE001
                self.record(fn.__name__, False, f"异常: {type(e).__name__}: {e}")
        (DATA / "report.json").write_text(
            json.dumps(self.results, ensure_ascii=False, indent=2), encoding="utf-8")
        ok = sum(1 for r in self.results if r["ok"])
        print(f"\n=== {ok}/{len(self.results)} 通过 ===", flush=True)


async def main() -> None:
    DATA.mkdir(exist_ok=True)
    if os.environ.get("MODE") == "worker":
        await run_worker(os.environ["QUEUES"].split(","))
        return
    client = await Client.connect("localhost:7233")
    await Runner(client).all()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        os.environ["MODE"] = "worker"
        q = sys.argv[3] if len(sys.argv) > 3 else "orchestrator"
        os.environ["QUEUES"] = q
    asyncio.run(main())
