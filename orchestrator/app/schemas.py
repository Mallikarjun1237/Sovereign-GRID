from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict
from app.models import NodeStatus, JobStatus, TaskStatus

# --- Node Schemas ---
class NodeSpecs(BaseModel):
    cores: int
    ram_gb: float
    device_type: str = "cpu"

class NodeRegisterRequest(BaseModel):
    node_id: str
    specs: NodeSpecs

class HeartbeatRequest(BaseModel):
    node_id: str

class NodeResponse(BaseModel):
    node_id: str
    device_type: str
    cores: int
    ram_gb: float
    status: NodeStatus
    last_heartbeat: datetime

    model_config = ConfigDict(from_attributes=True)

# --- Task & Chunk Schemas ---
class TaskItem(BaseModel):
    task_id: str
    prompt: str

    model_config = ConfigDict(from_attributes=True)

class TaskChunkPollResponse(BaseModel):
    chunk_id: str
    job_id: str
    tasks: List[TaskItem]

class TaskResultItem(BaseModel):
    task_id: str
    output: str

class TaskResultSubmission(BaseModel):
    chunk_id: str
    node_id: str
    results: List[TaskResultItem]

# --- Job Schemas ---
class BatchJobCreate(BaseModel):
    title: str
    prompts: List[str]

class TaskDetailResponse(BaseModel):
    id: str
    prompt: str
    output: Optional[str]
    status: TaskStatus

    model_config = ConfigDict(from_attributes=True)

# class JobResponse(BaseModel):
#     id: str
#     title: str
#     status: JobStatus
#     total_tasks: int
#     completed_tasks: int
#     created_at: datetime
#     tasks: Optional[List[TaskDetailResponse]] = None

#     model_config = ConfigDict(from_attributes=True)

class JobResponse(BaseModel):
    id: str
    title: str
    status: JobStatus
    total_tasks: int
    completed_tasks: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)