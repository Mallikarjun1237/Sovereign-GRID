import asyncio
import json
import logging

from .config import WorkerConfig
from .connection import OrchestratorConnection
from .executor import TaskExecutor
from .resources import ResourceMonitor, ResourceSnapshot, detect_resources


logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, config: WorkerConfig, connection: OrchestratorConnection, executor: TaskExecutor):
        self.config = config
        self.connection = connection
        self.executor = executor
        self.resource_monitor = ResourceMonitor(config.node_id)
        self.current_task_id: str | None = None
        self.status = "STARTING"
        self.execution_state = "STARTING"
        self.registered = False
        self.latest_snapshot: ResourceSnapshot | None = None
        self.tasks_received = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.executor.set_task_state_callback(self._set_task_state)
        self.executor.set_task_received_callback(self._record_task_received)
        self.executor.set_result_callback(self._record_task_result)
        self.executor.set_metrics_provider(self.metrics_snapshot)

    def mark_disconnected(self) -> None:
        if self.status != "STOPPING":
            self.status = "DISCONNECTED"
        logger.warning(
            "Worker %s disconnected with current_task_id=%s",
            self.config.node_id,
            self.current_task_id,
        )

    def _set_task_state(self, task_id: str | None) -> None:
        self.current_task_id = task_id
        if task_id is None and self.status == "BUSY":
            self.status = "ONLINE"
            self.execution_state = "IDLE"
        elif task_id is not None:
            self.status = "BUSY"
            self.execution_state = "EXECUTING"

    def _record_task_received(self, task_id: str) -> None:
        self.tasks_received += 1
        self.current_task_id = task_id
        self.status = "BUSY"
        self.execution_state = "VALIDATING"

    def _record_task_result(self, status: str) -> None:
        if status == "success":
            self.successful_tasks += 1
        else:
            self.failed_tasks += 1

    def metrics_snapshot(self) -> dict[str, object]:
        resources = self.collect_resource_snapshot()
        return {
            "worker_id": self.config.node_id,
            "execution_state": self.execution_state,
            "current_task_id": self.current_task_id,
            "tasks_received": self.tasks_received,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "cpu_usage_percent": resources.cpu_usage_percent,
            "ram_usage_percent": resources.ram_usage_percent,
            "ram_available_gb": resources.ram_available_gb,
            "timestamp": resources.timestamp.isoformat(),
        }

    def collect_resource_snapshot(self) -> ResourceSnapshot:
        self.latest_snapshot = self.resource_monitor.collect(
            worker_status=self.status,
            current_task_id=self.current_task_id,
        )
        return self.latest_snapshot

    async def resource_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.resource_interval)
            snapshot = self.collect_resource_snapshot()
            logger.info("worker_resource_snapshot %s", json.dumps(snapshot.as_dict(), sort_keys=True))

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval)
            await self.connection.heartbeat(self.collect_resource_snapshot())
            logger.debug("Heartbeat acknowledged for %s", self.config.node_id)

    async def poll_loop(self) -> None:
        while True:
            chunk = await self.connection.poll()
            if chunk is None:
                await asyncio.sleep(self.config.poll_interval)
                continue

            logger.info(
                "Received chunk %s with %d task(s)",
                chunk.chunk_id,
                len(chunk.tasks),
            )
            self.execution_state = "EXECUTING"
            results = await self.executor.execute(chunk)
            self.execution_state = "SUBMITTING"
            acknowledgement = await self.connection.complete(chunk, results)
            logger.info("Chunk %s acknowledged: %s", chunk.chunk_id, acknowledgement)
            self.execution_state = "IDLE"

    async def run_session(self, stop_event: asyncio.Event) -> None:
        resources = detect_resources(self.config.device_type)
        self.status = "REGISTERING"
        self.execution_state = "REGISTERING"
        logger.info(
            "Registering worker %s on hostname %s (%d cores, %.2f GB RAM, GPU=%s, VRAM=%s GB)",
            self.config.node_id,
            resources.hostname,
            resources.cores,
            resources.ram_gb,
            resources.gpu_name or "none",
            resources.vram_gb if resources.vram_gb is not None else "unknown",
        )
        registration = await self.connection.register(resources)
        logger.info("Worker registered with status %s", registration.get("status", "unknown"))
        self.registered = True
        self.status = "ONLINE"
        self.execution_state = "IDLE"

        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        poll_task = asyncio.create_task(self.poll_loop())
        resource_task = asyncio.create_task(self.resource_loop())
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {heartbeat_task, poll_task, resource_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        failures: list[BaseException] = []
        if stop_task not in done:
            for task in done:
                try:
                    task.result()
                except BaseException as exc:
                    failures.append(exc)
        else:
            self.status = "STOPPING"
            logger.info(
                "Worker %s stopping with current_task_id=%s",
                self.config.node_id,
                self.current_task_id,
            )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if failures:
            raise failures[0]
