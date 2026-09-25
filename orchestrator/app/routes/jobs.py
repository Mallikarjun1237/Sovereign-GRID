import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import JobModel, TaskModel, ChunkModel, NodeModel, JobStatus, TaskStatus, NodeStatus
from app.schemas import BatchJobCreate, JobResponse

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])

@router.post("/submit", response_model=JobResponse)
async def submit_job(payload: BatchJobCreate, db: AsyncSession = Depends(get_db)):
    if not payload.prompts:
        raise HTTPException(status_code=400, detail="Prompts list cannot be empty")

    job_id = str(uuid.uuid4())
    total_tasks = len(payload.prompts)

    # 1. Create Job record
    job = JobModel(
        id=job_id,
        title=payload.title,
        status=JobStatus.PROCESSING,
        total_tasks=total_tasks,
        completed_tasks=0,
        created_at=datetime.now(timezone.utc)
    )
    db.add(job)

    # 2. Get active nodes to determine chunking weights
    result = await db.execute(select(NodeModel).where(NodeModel.status == NodeStatus.ONLINE))
    active_nodes = result.scalars().all()

    # Determine default chunk size (5 items per chunk if no nodes, or balanced across nodes)
    chunk_size = 5 if not active_nodes else max(1, total_tasks // (len(active_nodes) * 2 or 1))

    # 3. Create Chunks and individual Tasks
    prompts = payload.prompts
    for i in range(0, total_tasks, chunk_size):
        chunk_slice = prompts[i:i + chunk_size]
        chunk_id = str(uuid.uuid4())

        chunk = ChunkModel(
            id=chunk_id,
            job_id=job_id,
            status=TaskStatus.PENDING,
            created_at=datetime.now(timezone.utc)
        )
        db.add(chunk)

        for prompt_text in chunk_slice:
            task = TaskModel(
                id=str(uuid.uuid4()),
                job_id=job_id,
                chunk_id=chunk_id,
                prompt=prompt_text,
                status=TaskStatus.PENDING
            )
            db.add(task)

    await db.commit()
    await db.refresh(job)
    return job

@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(JobModel).where(JobModel.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job