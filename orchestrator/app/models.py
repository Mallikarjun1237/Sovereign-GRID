import enum
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import String, Integer, Float, Text, ForeignKey, Enum, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class NodeStatus(str, enum.Enum):
    ONLINE = "ONLINE"
    BUSY = "BUSY"
    DEAD = "DEAD"

class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class NodeModel(Base):
    __tablename__ = "nodes"

    node_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    device_type: Mapped[str] = mapped_column(String(32), default="cpu")  # "cpu", "cuda", "metal"
    cores: Mapped[int] = mapped_column(Integer, default=4)
    ram_gb: Mapped[float] = mapped_column(Float, default=8.0)
    status: Mapped[NodeStatus] = mapped_column(Enum(NodeStatus), default=NodeStatus.ONLINE)
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(timezone.utc)
    )

    chunks: Mapped[List["ChunkModel"]] = relationship(back_populates="assigned_node")

class JobModel(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID
    title: Mapped[str] = mapped_column(String(255))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.PENDING)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    completed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(timezone.utc)
    )

    chunks: Mapped[List["ChunkModel"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    tasks: Mapped[List["TaskModel"]] = relationship(back_populates="job", cascade="all, delete-orphan")

class ChunkModel(Base):
    __tablename__ = "chunks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    node_id: Mapped[Optional[str]] = mapped_column(ForeignKey("nodes.node_id"), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(Enum(TaskStatus), default=TaskStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(timezone.utc)
    )

    job: Mapped["JobModel"] = relationship(back_populates="chunks")
    assigned_node: Mapped[Optional["NodeModel"]] = relationship(back_populates="chunks")
    tasks: Mapped[List["TaskModel"]] = relationship(back_populates="chunk")

class TaskModel(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    chunk_id: Mapped[Optional[str]] = mapped_column(ForeignKey("chunks.id", ondelete="SET NULL"), nullable=True)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[TaskStatus] = mapped_column(Enum(TaskStatus), default=TaskStatus.PENDING)

    job: Mapped["JobModel"] = relationship(back_populates="tasks")
    chunk: Mapped[Optional["ChunkModel"]] = relationship(back_populates="tasks")