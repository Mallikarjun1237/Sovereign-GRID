from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select, func
from app.database import AsyncSessionLocal
from app.models import NodeModel, JobModel, TaskModel, TaskStatus, NodeStatus
import asyncio

router = APIRouter(tags=["Telemetry"])

@router.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            async with AsyncSessionLocal() as db:
                # 1. Fetch current node states
                nodes_res = await db.execute(select(NodeModel))
                nodes = nodes_res.scalars().all()

                # 2. Fetch aggregate job stats
                tasks_completed_res = await db.execute(
                    select(func.count(TaskModel.id)).where(TaskModel.status == TaskStatus.COMPLETED)
                )
                tasks_completed = tasks_completed_res.scalar() or 0

                tasks_pending_res = await db.execute(
                    select(func.count(TaskModel.id)).where(TaskModel.status != TaskStatus.COMPLETED)
                )
                tasks_pending = tasks_pending_res.scalar() or 0

                payload = {
                    "nodes": [
                        {
                            "node_id": n.node_id,
                            "device_type": n.device_type,
                            "cores": n.cores,
                            "ram_gb": n.ram_gb,
                            "status": n.status.value,
                            "last_heartbeat": n.last_heartbeat.isoformat()
                        }
                        for n in nodes
                    ],
                    "metrics": {
                        "tasks_completed": tasks_completed,
                        "tasks_pending": tasks_pending,
                        "total_active_nodes": len([n for n in nodes if n.status == NodeStatus.ONLINE])
                    }
                }

                await websocket.send_json(payload)

            await asyncio.sleep(1)  # Stream updates once per second
    except WebSocketDisconnect:
        print("[-] Dashboard client disconnected from telemetry stream.")