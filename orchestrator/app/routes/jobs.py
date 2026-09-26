import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import JobModel, TaskModel, ChunkModel, NodeModel, JobStatus, TaskStatus, NodeStatus
from app.schemas import BatchJobCreate, JobResponse
from app.scheduler import create_proportional_chunks

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])

@router.post("/submit", response_model=JobResponse)
async def submit_job(payload: BatchJobCreate, db: AsyncSession = Depends(get_db)):
    if not payload.prompts:
        raise HTTPException(status_code=400, detail="Prompts list cannot be empty")

    job_id = str(uuid.uuid4())
    total_tasks = len(payload.prompts)
    now = datetime.now(timezone.utc)

    # 1. Instantiate the Job record
    job = JobModel(
        id=job_id,
        title=payload.title,
        status=JobStatus.PROCESSING,
        total_tasks=total_tasks,
        completed_tasks=0,
        created_at=now
    )
    db.add(job)

    # 2. Get active nodes for allocation
    result = await db.execute(select(NodeModel).where(NodeModel.status == NodeStatus.ONLINE))
    active_nodes = result.scalars().all()

    # 3. Create proportional slices
    planned_chunks = create_proportional_chunks(payload.prompts, active_nodes)

    # 4. Insert Chunks and Tasks
    for chunk_data in planned_chunks:
        chunk_id = str(uuid.uuid4())
        target_node = chunk_data.get("target_node_id")

        chunk = ChunkModel(
            id=chunk_id,
            job_id=job_id,
            node_id=target_node,
            status=TaskStatus.PENDING,
            created_at=now
        )
        db.add(chunk)

        for prompt_text in chunk_data.get("prompts", []):
            task = TaskModel(
                id=str(uuid.uuid4()),
                job_id=job_id,
                chunk_id=chunk_id,
                prompt=prompt_text,
                status=TaskStatus.PENDING
            )
            db.add(task)

    # Commit all models in a single atomic transaction
    await db.commit()

    # Return explicit response (avoids async lazy-load / refresh attribute crashes)
    return JobResponse(
        id=job_id,
        title=payload.title,
        status=JobStatus.PROCESSING,
        total_tasks=total_tasks,
        completed_tasks=0,
        created_at=now
    )

@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(JobModel).where(JobModel.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobResponse(
        id=job.id,
        title=job.title,
        status=job.status,
        total_tasks=job.total_tasks,
        completed_tasks=job.completed_tasks,
        created_at=job.created_at
    )


@router.get("/{job_id}/results")
async def get_job_results(job_id: str, db: AsyncSession = Depends(get_db)):
    """Fetches all completed task outputs for a given job."""
    tasks_res = await db.execute(
        select(TaskModel)
        .where(TaskModel.job_id == job_id)
        .order_by(TaskModel.id.asc())
    )
    tasks = tasks_res.scalars().all()
    if not tasks:
        raise HTTPException(status_code=404, detail="No tasks found for this job")

    return [
        {
            "task_id": t.id,
            "prompt": t.prompt,
            "output": t.output,
            "status": t.status.value
        }
        for t in tasks
    ]