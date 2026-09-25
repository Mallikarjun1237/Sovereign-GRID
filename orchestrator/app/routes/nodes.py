from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import NodeModel, NodeStatus
from app.schemas import NodeRegisterRequest, HeartbeatRequest, NodeResponse

router = APIRouter(prefix="/api/nodes", tags=["Nodes"])

@router.post("/register", response_model=NodeResponse)
async def register_node(payload: NodeRegisterRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(NodeModel).where(NodeModel.node_id == payload.node_id))
    node = result.scalar_one_or_none()

    if not node:
        node = NodeModel(
            node_id=payload.node_id,
            device_type=payload.specs.device_type,
            cores=payload.specs.cores,
            ram_gb=payload.specs.ram_gb,
            status=NodeStatus.ONLINE,
            last_heartbeat=datetime.now(timezone.utc)
        )
        db.add(node)
    else:
        node.status = NodeStatus.ONLINE
        node.device_type = payload.specs.device_type
        node.cores = payload.specs.cores
        node.ram_gb = payload.specs.ram_gb
        node.last_heartbeat = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(node)
    return node

@router.post("/heartbeat")
async def heartbeat(payload: HeartbeatRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(NodeModel).where(NodeModel.node_id == payload.node_id))
    node = result.scalar_one_or_none()
    if not node:
        raise HTTPException(status_code=404, detail="Node not registered")
    
    node.last_heartbeat = datetime.now(timezone.utc)
    node.status = NodeStatus.ONLINE
    await db.commit()
    return {"status": "alive"}

@router.get("", response_model=list[NodeResponse])
async def list_nodes(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(NodeModel))
    return result.scalars().all()