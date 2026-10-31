"""Temporal 冒烟测试：证明 client → server → worker → activity 整条链路通。

用法（先确保另一个窗口里 `temporal server start-dev --db-filename .\\temporal.db` 正在跑）：
    python temporal-smoke.py
"""
import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

TASK_QUEUE = "smoke"


@activity.defn
async def shout(text: str) -> str:
    print(f"  [activity] 收到 {text!r}，睡 1 秒")
    await asyncio.sleep(1)
    return text.upper()


@workflow.defn
class Greet:
    @workflow.run
    async def run(self, name: str) -> str:
        result = await workflow.execute_activity(
            shout,
            name,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return f"hello, {result}"


async def main() -> None:
    client = await Client.connect("localhost:7233")
    print("connected:", client.service_client.config.target_host)

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[Greet],
        activities=[shout],
    ):
        out = await client.execute_workflow(
            Greet.run,
            "world",
            id="smoke-1",
            task_queue=TASK_QUEUE,
        )
        print("RESULT:", out)


if __name__ == "__main__":
    asyncio.run(main())
