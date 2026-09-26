from typing import Any

import httpx

from .config import WorkerConfig
from .resources import ResourceSnapshot, WorkerResources
from .serializer import TaskChunk, deserialize_chunk, serialize_completion, TaskResult


class OrchestratorConnection:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            base_url=config.orchestrator_url,
            timeout=config.request_timeout,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def register(self, resources: WorkerResources) -> dict[str, Any]:
        response = await self.client.post(
            "/api/nodes/register",
            json={
                "node_id": self.config.node_id,
                "specs": {
                    "cores": resources.cores,
                    "ram_gb": resources.ram_gb,
                    "device_type": resources.device_type,
                },
            },
        )
        response.raise_for_status()
        return response.json()

    async def heartbeat(self, snapshot: ResourceSnapshot | None = None) -> dict[str, Any]:
        if snapshot is not None:
            self._log_resource_snapshot(snapshot)
        response = await self.client.post(
            "/api/nodes/heartbeat",
            json={"node_id": self.config.node_id},
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _log_resource_snapshot(snapshot: ResourceSnapshot) -> None:
        import json
        import logging

        logging.getLogger(__name__).info(
            "worker_resource_snapshot %s",
            json.dumps(snapshot.as_dict(), sort_keys=True),
        )

    async def poll(self) -> TaskChunk | None:
        response = await self.client.get(
            "/api/tasks/poll",
            params={"node_id": self.config.node_id},
        )
        response.raise_for_status()
        return deserialize_chunk(response.json())

    async def complete(self, chunk: TaskChunk, results: list[TaskResult]) -> dict[str, Any]:
        response = await self.client.post(
            "/api/tasks/complete",
            json=serialize_completion(chunk, self.config.node_id, results),
        )
        response.raise_for_status()
        return response.json()
