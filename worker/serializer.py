from dataclasses import dataclass
import json
import math
from typing import Any


class ProtocolError(ValueError):
    """Raised when the orchestrator returns an unexpected payload."""


@dataclass(frozen=True)
class Task:
    task_id: str
    prompt: str


@dataclass(frozen=True)
class TaskChunk:
    chunk_id: str
    job_id: str
    tasks: list[Task]


@dataclass(frozen=True)
class TaskResult:
    task_id: str
    output: str


def _require_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{name} must be a non-empty string")
    return value


def deserialize_chunk(payload: Any) -> TaskChunk | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ProtocolError("Task poll response must be an object or null")

    try:
        raw_tasks = payload["tasks"]
        if not isinstance(raw_tasks, list):
            raise ProtocolError("Task chunk 'tasks' must be a list")
        tasks = []
        for item in raw_tasks:
            if not isinstance(item, dict):
                raise ProtocolError("Each task must be an object")
            task_id = item["task_id"]
            prompt = item["prompt"]
            if not isinstance(task_id, str) or not task_id:
                raise ProtocolError("Each task_id must be a non-empty string")
            if not isinstance(prompt, str):
                raise ProtocolError("Each prompt must be a string")
            tasks.append(Task(task_id=task_id, prompt=prompt))
        return TaskChunk(
            chunk_id=_require_identifier(payload["chunk_id"], "chunk_id"),
            job_id=_require_identifier(payload["job_id"], "job_id"),
            tasks=tasks,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ProtocolError(f"Invalid task chunk payload: {exc}") from exc


def serialize_completion(chunk: TaskChunk, node_id: str, results: list[TaskResult]) -> dict[str, Any]:
    if not isinstance(results, list):
        raise ProtocolError("results must be a list")
    payload = {
        "chunk_id": chunk.chunk_id,
        "node_id": _require_identifier(node_id, "node_id"),
        "results": [
            {
                "task_id": _require_identifier(result.task_id, "result.task_id"),
                "output": result.output,
            }
            for result in results
        ],
    }
    _require_identifier(chunk.chunk_id, "chunk_id")
    _require_identifier(chunk.job_id, "job_id")
    for result in results:
        if not isinstance(result.output, str):
            raise ProtocolError("result.output must be a string")

    try:
        return json.loads(
            json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=True)
        )
    except (TypeError, ValueError) as exc:
        raise ProtocolError("Completion payload is not JSON serializable") from exc


def validate_processing_time(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ProtocolError("processing_time_ms must be a finite number")
    return float(value)
