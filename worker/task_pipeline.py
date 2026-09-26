from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import time
from collections.abc import Callable
from typing import Protocol

from .runtime import InferenceResult, InferenceRuntime
from .serializer import Task, validate_processing_time


logger = logging.getLogger(__name__)


class TaskValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedTask:
    task_id: str
    operation: str
    model: str
    input_text: str


@dataclass(frozen=True)
class TaskExecutionResult:
    worker_id: str
    task_id: str
    status: str
    processing_time_ms: float
    output: str | None = None
    error: dict[str, str] | None = None
    model: str | None = None
    task_received_at: str = ""
    execution_started_at: str | None = None
    task_completed_at: str = ""
    inference_duration_ms: float = 0.0
    worker_metrics: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if self.status not in {"success", "failure"}:
            raise TaskValidationError("result status must be success or failure")
        if not isinstance(self.worker_id, str) or not self.worker_id:
            raise TaskValidationError("result worker_id must be a non-empty string")
        if not isinstance(self.task_id, str) or not self.task_id:
            raise TaskValidationError("result task_id must be a non-empty string")
        validate_processing_time(self.processing_time_ms)
        if self.output is not None and not isinstance(self.output, str):
            raise TaskValidationError("result output must be a string or null")
        if self.model is not None and not isinstance(self.model, str):
            raise TaskValidationError("result model must be a string or null")
        if not self.task_received_at or not self.task_completed_at:
            raise TaskValidationError("result timestamps are required")
        if self.execution_started_at is not None and not isinstance(self.execution_started_at, str):
            raise TaskValidationError("execution_started_at must be a string or null")
        validate_processing_time(self.inference_duration_ms)
        if self.worker_metrics is not None and not isinstance(self.worker_metrics, dict):
            raise TaskValidationError("worker_metrics must be an object or null")
        if self.error is not None:
            if not isinstance(self.error, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in self.error.items()
            ):
                raise TaskValidationError("result error must contain only string fields")

    def to_backend_result(self) -> dict[str, str]:
        envelope = {
            "worker_id": self.worker_id,
            "task_id": self.task_id,
            "status": self.status,
            "processing_time_ms": self.processing_time_ms,
            "model": self.model,
            "output": self.output,
            "error": self.error,
            "timing": {
                "task_received_at": self.task_received_at,
                "execution_started_at": self.execution_started_at,
                "task_completed_at": self.task_completed_at,
                "inference_duration_ms": self.inference_duration_ms,
                "total_processing_duration_ms": self.processing_time_ms,
            },
            "worker_metrics": self.worker_metrics,
        }
        try:
            serialized = json.dumps(
                envelope,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as exc:
            raise TaskValidationError("result envelope is not JSON serializable") from exc
        return {"task_id": self.task_id, "output": serialized}


class TaskOperation(Protocol):
    async def execute(self, task: ValidatedTask) -> InferenceResult:
        ...


class GenerateOperation:
    def __init__(self, runtime: InferenceRuntime):
        self.runtime = runtime

    async def execute(self, task: ValidatedTask) -> InferenceResult:
        return await self.runtime.execute(task.input_text)


class OperationRegistry:
    def __init__(self, operations: dict[str, TaskOperation] | None = None):
        self._operations = operations or {}

    def register(self, name: str, operation: TaskOperation) -> None:
        self._operations[name] = operation

    def get(self, name: str) -> TaskOperation:
        try:
            return self._operations[name]
        except KeyError as exc:
            raise TaskValidationError(f"Unsupported task operation: {name}") from exc


class TaskProcessor:
    def __init__(
        self,
        worker_id: str,
        model: str,
        runtime: InferenceRuntime,
        operation_name: str = "generate",
        registry: OperationRegistry | None = None,
    ):
        self.worker_id = worker_id
        self.model = model
        self.operation_name = operation_name
        self.registry = registry or OperationRegistry()
        self.registry.register(operation_name, GenerateOperation(runtime))
        self._task_state_callback: Callable[[str | None], None] | None = None
        self._task_received_callback: Callable[[str], None] | None = None

    def set_task_state_callback(self, callback: Callable[[str | None], None]) -> None:
        self._task_state_callback = callback

    def set_task_received_callback(self, callback: Callable[[str], None]) -> None:
        self._task_received_callback = callback

    def validate(self, task: Task) -> ValidatedTask:
        if not isinstance(task.task_id, str) or not task.task_id:
            raise TaskValidationError("task_id must be a non-empty string")
        if not isinstance(task.prompt, str) or not task.prompt.strip():
            raise TaskValidationError("prompt must be a non-empty string")
        return ValidatedTask(
            task_id=task.task_id,
            operation=self.operation_name,
            model=self.model,
            input_text=task.prompt,
        )

    async def process(self, task: Task) -> TaskExecutionResult:
        logger.info("task_received worker_id=%s task_id=%s", self.worker_id, task.task_id)
        started_at = time.perf_counter()
        task_received_at = datetime.now(timezone.utc).isoformat()
        execution_started_at: str | None = None
        inference_duration_ms = 0.0
        task_id = task.task_id if isinstance(task.task_id, str) else "invalid-task-id"
        if self._task_received_callback is not None and isinstance(task.task_id, str):
            self._task_received_callback(task.task_id)
        try:
            validated = self.validate(task)
            execution_started_at = datetime.now(timezone.utc).isoformat()
            if self._task_state_callback is not None:
                self._task_state_callback(validated.task_id)
            logger.info(
                "task_started worker_id=%s task_id=%s operation=%s model=%s",
                self.worker_id,
                validated.task_id,
                validated.operation,
                validated.model,
            )
            inference_started_at = time.perf_counter()
            try:
                inference = await self.registry.get(validated.operation).execute(validated)
            finally:
                inference_duration_ms = round((time.perf_counter() - inference_started_at) * 1000, 2)
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            result = TaskExecutionResult(
                worker_id=self.worker_id,
                task_id=validated.task_id,
                status="success",
                processing_time_ms=duration_ms,
                output=inference.output,
                model=inference.model,
                task_received_at=task_received_at,
                execution_started_at=execution_started_at,
                task_completed_at=datetime.now(timezone.utc).isoformat(),
                inference_duration_ms=inference.duration_ms or inference_duration_ms,
            )
            logger.info(
                "task_completed worker_id=%s task_id=%s processing_time_ms=%.2f",
                self.worker_id,
                validated.task_id,
                duration_ms,
            )
            return result
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            task_completed_at = datetime.now(timezone.utc).isoformat()
            error = self._safe_error(exc)
            logger.error(
                "task_failed worker_id=%s task_id=%s error_type=%s processing_time_ms=%.2f",
                self.worker_id,
                task_id,
                type(exc).__name__,
                duration_ms,
            )
            return TaskExecutionResult(
                worker_id=self.worker_id,
                task_id=task_id,
                status="failure",
                processing_time_ms=duration_ms,
                error=error,
                model=self.model,
                task_received_at=task_received_at,
                execution_started_at=execution_started_at,
                task_completed_at=task_completed_at,
                inference_duration_ms=inference_duration_ms,
            )
        finally:
            if self._task_state_callback is not None:
                self._task_state_callback(None)

    @staticmethod
    def _safe_error(exc: Exception) -> dict[str, str]:
        if isinstance(exc, TaskValidationError):
            return {"type": "validation_error", "message": str(exc)}
        if isinstance(exc, TimeoutError):
            return {"type": "timeout", "message": "Task execution timed out"}
        if getattr(exc, "safe_for_response", False):
            return {"type": type(exc).__name__, "message": str(exc)}
        return {
            "type": "execution_error",
            "message": "Task execution failed unexpectedly",
        }
