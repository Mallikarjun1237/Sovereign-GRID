# from datetime import datetime, timezone
# from fastapi import APIRouter, Depends, HTTPException
# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy import select
# from app.database import get_db
# from app.models import ChunkModel, TaskModel, JobModel, NodeModel, TaskStatus, JobStatus, NodeStatus
# from app.schemas import TaskChunkPollResponse, TaskResultSubmission, TaskItem

# router = APIRouter(prefix="/api/tasks", tags=["Tasks"])

# @router.get("/poll", response_model=TaskChunkPollResponse | None)
# async def poll_task(node_id: str, db: AsyncSession = Depends(get_db)):
#     # Verify node exists and is active
#     node_res = await db.execute(select(NodeModel).where(NodeModel.node_id == node_id))
#     node = node_res.scalar_one_or_none()
#     if not node or node.status == NodeStatus.DEAD:
#         raise HTTPException(status_code=403, detail="Node unregistered or marked dead")

#     # Find the oldest pending chunk
#     chunk_res = await db.execute(
#         select(ChunkModel)
#         .where(ChunkModel.status == TaskStatus.PENDING)
#         .order_by(ChunkModel.created_at.asc())
#         .limit(1)
#     )
#     chunk = chunk_res.scalar_one_or_none()

#     if not chunk:
#         return None  # No work currently available

#     # Lock chunk to this node
#     chunk.status = TaskStatus.IN_PROGRESS
#     chunk.node_id = node_id

#     # Fetch associated tasks
#     tasks_res = await db.execute(select(TaskModel).where(TaskModel.chunk_id == chunk.id))
#     tasks = tasks_res.scalars().all()

#     for t in tasks:
#         t.status = TaskStatus.IN_PROGRESS

#     await db.commit()

#     return TaskChunkPollResponse(
#         chunk_id=chunk.id,
#         job_id=chunk.job_id,
#         tasks=[TaskItem(task_id=t.id, prompt=t.prompt) for t in tasks]
#     )

# @router.post("/complete")
# async def complete_chunk(payload: TaskResultSubmission, db: AsyncSession = Depends(get_db)):
#     chunk_res = await db.execute(select(ChunkModel).where(ChunkModel.id == payload.chunk_id))
#     chunk = chunk_res.scalar_one_or_none()
#     if not chunk:
#         raise HTTPException(status_code=404, detail="Chunk not found")

#     # Write task results to database
#     for res in payload.results:
#         task_res = await db.execute(select(TaskModel).where(TaskModel.id == res.task_id))
#         task = task_res.scalar_one_or_none()
#         if task:
#             task.output = res.output
#             task.status = TaskStatus.COMPLETED

#     chunk.status = TaskStatus.COMPLETED

#     # Update Job progress counter
#     job_res = await db.execute(select(JobModel).where(JobModel.id == chunk.job_id))
#     job = job_res.scalar_one_or_none()
#     if job:
#         job.completed_tasks += len(payload.results)
#         if job.completed_tasks >= job.total_tasks:
#             job.status = JobStatus.COMPLETED

#     await db.commit()
#     return {"status": "success", "processed_tasks": len(payload.results)}

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from app.database import get_db
from app.models import ChunkModel, TaskModel, NodeModel, JobModel, TaskStatus, NodeStatus, JobStatus
from app.schemas import TaskChunkPollResponse, TaskResultSubmission, TaskItem

router = APIRouter(prefix="/api/tasks", tags=["Tasks"])

@router.get("/poll", response_model=TaskChunkPollResponse | None)
async def poll_task(node_id: str, db: AsyncSession = Depends(get_db)):
    # 1. Verify node exists and is online
    node_res = await db.execute(select(NodeModel).where(NodeModel.node_id == node_id))
    node = node_res.scalar_one_or_none()
    if not node or node.status == NodeStatus.DEAD:
        raise HTTPException(status_code=403, detail="Node unregistered or marked dead")

    # 2. Find oldest pending chunk
    chunk_res = await db.execute(
        select(ChunkModel)
        .where(
            ChunkModel.status == TaskStatus.PENDING,
            (ChunkModel.node_id == node_id) | (ChunkModel.node_id.is_(None))
        )
        .order_by(ChunkModel.created_at.asc())
        .limit(1)
    )
    chunk = chunk_res.scalar_one_or_none()

    if not chunk:
        return None

    # 3. Lock chunk to this node
    chunk.status = TaskStatus.IN_PROGRESS
    chunk.node_id = node_id

    # 4. Fetch associated tasks
    tasks_res = await db.execute(select(TaskModel).where(TaskModel.chunk_id == chunk.id))
    tasks = tasks_res.scalars().all()

    for t in tasks:
        t.status = TaskStatus.IN_PROGRESS

    await db.commit()

    return TaskChunkPollResponse(
        chunk_id=str(chunk.id),
        job_id=str(chunk.job_id),
        tasks=[TaskItem(task_id=str(t.id), prompt=t.prompt) for t in tasks]
    )

@router.post("/complete")
async def complete_chunk(payload: TaskResultSubmission, db: AsyncSession = Depends(get_db)):
    chunk_res = await db.execute(select(ChunkModel).where(ChunkModel.id == payload.chunk_id))
    chunk = chunk_res.scalar_one_or_none()
    if not chunk:
        raise HTTPException(status_code=404, detail="Chunk not found")

    job_id = chunk.job_id

    # 1. Update task records with outputs and mark COMPLETED
    for res in payload.results:
        await db.execute(
            update(TaskModel)
            .where(TaskModel.id == str(res.task_id))
            .values(output=res.output, status=TaskStatus.COMPLETED)
        )

    # 2. Mark chunk as COMPLETED
    chunk.status = TaskStatus.COMPLETED

    # 3. Count total completed tasks directly from database for accuracy
    count_res = await db.execute(
        select(func.count(TaskModel.id)).where(
            TaskModel.job_id == job_id,
            TaskModel.status == TaskStatus.COMPLETED
        )
    )
    completed_count = count_res.scalar() or 0

    # 4. Update the Job record
    job_res = await db.execute(select(JobModel).where(JobModel.id == job_id))
    job = job_res.scalar_one_or_none()
    
    if job:
        job.completed_tasks = completed_count
        if job.completed_tasks >= job.total_tasks:
            job.status = JobStatus.COMPLETED

    await db.commit()
    return {"status": "success", "completed_count": completed_count}