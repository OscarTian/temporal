"""S9：起一个 8 步串行流程，等它在跑的时候被外部杀掉 worker。"""
import asyncio

from temporalio.client import Client

from flows import ToolFlow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    plan = {
        "scenario": "s9",
        "concurrency": 1,
        "steps": [
            {"id": f"k{i}", "tool": "run", "needs": [], "queue": "host-a",
             "params": {"sleep": 2.0}}
            for i in range(6)
        ],
    }
    handle = await client.start_workflow(
        ToolFlow.run, plan, id="s9-kill-2", task_queue="orchestrator"
    )
    out = await handle.result()
    print("RESULT:", out)


if __name__ == "__main__":
    asyncio.run(main())

