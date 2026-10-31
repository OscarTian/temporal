"""把 Temporal UI 上看到的东西，用 API 原样抓出来。

对应 UI 的 Timeline 页：DescribeWorkflowExecution + GetWorkflowExecutionHistory

用法：
    python dump-history.py                          # 默认抓 smoke-1 的最新一次
    python dump-history.py --workflow-id s9-kill-2  # 抓指定的
    python dump-history.py --workflow-id x --run-id 01a1...   # 抓指定的某一次 run
"""
import argparse
import asyncio
import base64

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client


def decode_payload(p) -> str:
    """payload.data 是原始字节（HTTP/JSON 网关才用 base64），metadata 里记着编码。"""
    enc = p.metadata.get("encoding", "?")
    if isinstance(enc, bytes):
        enc = enc.decode()
    raw = p.data if isinstance(p.data, bytes) else base64.b64decode(p.data)
    if enc.startswith("json"):
        return f"{enc}: {raw.decode()}"
    return f"{enc}: {len(raw)} bytes"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow-id", default="smoke-1")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(args.workflow_id, run_id=args.run_id)

    d = await handle.describe()
    print("=== DescribeWorkflowExecution（UI 顶部那些字段）===")
    print(f"  status      : {d.status.name}")
    print(f"  start_time  : {d.start_time}")
    print(f"  close_time  : {d.close_time}")
    print(f"  task_queue  : {d.task_queue}")
    print(f"  workflow_id : {d.id}")
    print(f"  run_id      : {d.run_id}")

    hist = await handle.fetch_history()
    print(f"\n=== GetWorkflowExecutionHistory：共 {len(hist.events)} 条事件 ===")
    for e in hist.events:
        ts = e.event_time.ToDatetime().strftime("%H:%M:%S.%f")[:-3] if e.event_time else ""
        print(f"  {e.event_id:>3}  {EventType.Name(e.event_type):<38} {ts}")

    print("\n=== 输入 / 输出（UI 里那个 base64 框）===")
    for e in hist.events:
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
            for p in e.workflow_execution_started_event_attributes.input.payloads:
                print("  input :", decode_payload(p))
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            for p in e.activity_task_completed_event_attributes.result.payloads:
                print("  act-out:", decode_payload(p))
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
            for p in e.workflow_execution_completed_event_attributes.result.payloads:
                print("  output:", decode_payload(p))


if __name__ == "__main__":
    asyncio.run(main())
