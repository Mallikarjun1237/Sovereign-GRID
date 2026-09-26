from collections.abc import Callable
from dataclasses import replace

from .runtime import InferenceRuntime
from .serializer import TaskChunk, TaskResult
from .task_pipeline import OperationRegistry, TaskProcessor


class TaskExecutor:
    def __init__(
        self,
        worker_id: str,
        model: str,
        runtime: InferenceRuntime,
        registry: OperationRegistry | None = None,
    ):
        self.processor = TaskProcessor(worker_id, model, runtime, registry=registry)
        self._metrics_provider: Callable[[], dict[str, object]] | None = None
        self._result_callback: Callable[[str], None] | None = None

    def set_task_state_callback(self, callback: Callable[[str | None], None]) -> None:
        self.processor.set_task_state_callback(callback)

    def set_metrics_provider(self, provider: Callable[[], dict[str, object]]) -> None:
        self._metrics_provider = provider

    def set_task_received_callback(self, callback: Callable[[str], None]) -> None:
        self.processor.set_task_received_callback(callback)

    def set_result_callback(self, callback: Callable[[str], None]) -> None:
        self._result_callback = callback

    async def execute(self, chunk: TaskChunk) -> list[TaskResult]:
        results: list[TaskResult] = []
        for task in chunk.tasks:
            execution_result = await self.processor.process(task)
            if self._result_callback is not None:
                self._result_callback(execution_result.status)
            if self._metrics_provider is not None:
                execution_result = replace(
                    execution_result,
                    worker_metrics=self._metrics_provider(),
                )
            results.append(TaskResult(**execution_result.to_backend_result()))
        return results
