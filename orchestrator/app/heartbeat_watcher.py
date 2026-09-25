import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update
from app.database import AsyncSessionLocal
from app.models import NodeModel, ChunkModel, TaskModel, NodeStatus, TaskStatus

HEARTBEAT_TIMEOUT_SECONDS = 8
CHECK_INTERVAL_SECONDS = 3

async def watch_node_heartbeats():
    """Continuously checks for dropped nodes and re-queues unfinished chunks."""
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)

            async with AsyncSessionLocal() as db:
                # 1. Locate nodes that haven't pulsed since the cutoff
                dead_nodes_query = select(NodeModel).where(
                    NodeModel.status != NodeStatus.DEAD,
                    NodeModel.last_heartbeat < cutoff
                )
                dead_nodes_res = await db.execute(dead_nodes_query)
                dead_nodes = dead_nodes_res.scalars().all()

                for node in dead_nodes:
                    print(f"[!] Node '{node.node_id}' timed out! Marking as DEAD.")
                    node.status = NodeStatus.DEAD

                    # 2. Find any chunks that were actively assigned to this dead node
                    stranded_chunks_res = await db.execute(
                        select(ChunkModel).where(
                            ChunkModel.node_id == node.node_id,
                            ChunkModel.status == TaskStatus.IN_PROGRESS
                        )
                    )
                    stranded_chunks = stranded_chunks_res.scalars().all()

                    for chunk in stranded_chunks:
                        print(f"[*] Re-queuing orphaned chunk {chunk.id} from node {node.node_id}")
                        chunk.status = TaskStatus.PENDING
                        chunk.node_id = None  # Detach worker

                        # 3. Reset associated tasks back to PENDING
                        await db.execute(
                            update(TaskModel)
                            .where(TaskModel.chunk_id == chunk.id)
                            .values(status=TaskStatus.PENDING)
                        )

                await db.commit()

        except Exception as e:
            print(f"[ERROR in heartbeat_watcher]: {e}")