import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
import threading
import time
import unittest
from urllib.parse import parse_qs, urlparse

from .config import WorkerConfig
from .connection import OrchestratorConnection
from .executor import TaskExecutor
from .runtime import InferenceResult
from .worker import Worker


HEARTBEAT_TIMEOUT_SECONDS = 0.8
TASK_COUNT = 4


class FailureOrchestratorState:
    def __init__(self):
        self.lock = threading.RLock()
        self.nodes: dict[str, dict] = {}
        self.chunks: list[dict] = []
        self.job_id = "failure-integration-job"
        self.requeued_chunk_ids: list[str] = []
        self.claim_history: list[tuple[str, str]] = []
        self.completed_by_node: dict[str, list[str]] = {}
        self.reaper_stop = threading.Event()
        self.reaper = threading.Thread(target=self._watch_heartbeats, daemon=True)

    def start(self) -> None:
        self.reaper.start()

    def stop(self) -> None:
        self.reaper_stop.set()
        self.reaper.join(timeout=2)

    def _watch_heartbeats(self) -> None:
        while not self.reaper_stop.wait(0.05):
            now = time.monotonic()
            with self.lock:
                for node_id, node in self.nodes.items():
                    if node["status"] == "DEAD":
                        continue
                    if now - node["last_heartbeat"] <= HEARTBEAT_TIMEOUT_SECONDS:
                        continue
                    node["status"] = "DEAD"
                    for chunk in self.chunks:
                        if chunk["node_id"] == node_id and chunk["status"] == "IN_PROGRESS":
                            chunk["status"] = "PENDING"
                            chunk["node_id"] = None
                            self.requeued_chunk_ids.append(chunk["chunk_id"])

    def create_job(self, prompts: list[str]) -> dict:
        with self.lock:
            self.chunks = [
                {
                    "chunk_id": f"failure-integration-chunk-{index}",
                    "job_id": self.job_id,
                    "node_id": None,
                    "status": "PENDING",
                    "task": {
                        "task_id": f"failure-integration-task-{index}",
                        "prompt": prompt,
                    },
                }
                for index, prompt in enumerate(prompts, start=1)
            ]
        return {
            "id": self.job_id,
            "title": "Worker Failure Integration Test",
            "status": "PROCESSING",
            "total_tasks": len(prompts),
            "completed_tasks": 0,
            "created_at": "2026-09-26T00:00:00+00:00",
        }

    def job_status(self) -> dict:
        with self.lock:
            completed = sum(chunk["status"] == "COMPLETED" for chunk in self.chunks)
            return {
                "id": self.job_id,
                "title": "Worker Failure Integration Test",
                "status": "COMPLETED" if completed == len(self.chunks) else "PROCESSING",
                "total_tasks": len(self.chunks),
                "completed_tasks": completed,
                "created_at": "2026-09-26T00:00:00+00:00",
            }


class FailureOrchestratorHandler(BaseHTTPRequestHandler):
    state: FailureOrchestratorState
    server_version = "SovereignGRIDFailureTestOrchestrator/1.0"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _payload(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length))

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        payload = self._payload()
        if path == "/api/nodes/register":
            node_id = payload["node_id"]
            with self.state.lock:
                self.state.nodes[node_id] = {
                    "status": "ONLINE",
                    "last_heartbeat": time.monotonic(),
                }
            self._json(
                200,
                {
                    "node_id": node_id,
                    "device_type": payload["specs"]["device_type"],
                    "cores": payload["specs"]["cores"],
                    "ram_gb": payload["specs"]["ram_gb"],
                    "status": "ONLINE",
                    "last_heartbeat": "2026-09-26T00:00:00+00:00",
                },
            )
            return
        if path == "/api/nodes/heartbeat":
            with self.state.lock:
                node = self.state.nodes.get(payload["node_id"])
                if node is not None:
                    node["last_heartbeat"] = time.monotonic()
                    node["status"] = "ONLINE"
            self._json(200, {"status": "alive"})
            return
        if path == "/api/jobs/submit":
            self._json(200, self.state.create_job(payload["prompts"]))
            return
        if path == "/api/tasks/complete":
            with self.state.lock:
                chunk = next(
                    chunk for chunk in self.state.chunks if chunk["chunk_id"] == payload["chunk_id"]
                )
                chunk["status"] = "COMPLETED"
                node_id = payload["node_id"]
                self.state.completed_by_node.setdefault(node_id, []).extend(
                    result["task_id"] for result in payload["results"]
                )
            self._json(200, {"status": "success", "completed_count": 1})
            return
        self._json(404, {"detail": "not found"})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/tasks/poll":
            node_id = parse_qs(parsed.query)["node_id"][0]
            with self.state.lock:
                node = self.state.nodes.get(node_id)
                if node is None or node["status"] == "DEAD":
                    self._json(403, {"detail": "Node unregistered or marked dead"})
                    return
                chunk = next(
                    (
                        chunk
                        for chunk in self.state.chunks
                        if chunk["status"] == "PENDING"
                        and (chunk["node_id"] is None or chunk["node_id"] == node_id)
                    ),
                    None,
                )
                if chunk is None:
                    payload = None
                else:
                    chunk["status"] = "IN_PROGRESS"
                    chunk["node_id"] = node_id
                    self.state.claim_history.append((node_id, chunk["chunk_id"]))
                    payload = {
                        "chunk_id": chunk["chunk_id"],
                        "job_id": chunk["job_id"],
                        "tasks": [chunk["task"]],
                    }
            self._json(200, payload)
            return
        if parsed.path.startswith("/api/jobs/"):
            self._json(200, self.state.job_status())
            return
        self._json(404, {"detail": "not found"})


class DeterministicFailureRuntime:
    def __init__(self, delay_seconds: float):
        self.delay_seconds = delay_seconds

    async def execute(self, prompt: str) -> InferenceResult:
        await asyncio.sleep(self.delay_seconds)
        return InferenceResult(
            output=f"completed:{prompt}",
            model="deterministic-failure-test-runtime",
            duration_ms=self.delay_seconds * 1000,
        )


def run_test_worker(base_url: str, worker_id: str, delay_seconds: float) -> None:
    async def run() -> None:
        config = WorkerConfig(
            orchestrator_url=base_url,
            node_id=worker_id,
            heartbeat_interval=0.1,
            poll_interval=0.05,
            resource_interval=0.1,
            request_timeout=2,
        )
        connection = OrchestratorConnection(config)
        worker = Worker(
            config,
            connection,
            TaskExecutor(worker_id, "deterministic-failure-test-runtime", DeterministicFailureRuntime(delay_seconds)),
        )
        try:
            await worker.run_session(asyncio.Event())
        finally:
            await connection.close()

    asyncio.run(run())


class WorkerFailureIntegrationTest(unittest.TestCase):
    def test_worker_failure_is_detected_and_work_is_requeued_to_worker_two(self) -> None:
        state = FailureOrchestratorState()
        handler = type("BoundFailureHandler", (FailureOrchestratorHandler,), {"state": state})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        state.start()

        worker_one = multiprocessing.Process(
            target=run_test_worker,
            args=(f"http://127.0.0.1:{server.server_port}", "worker-1", 5.0),
        )
        worker_two = multiprocessing.Process(
            target=run_test_worker,
            args=(f"http://127.0.0.1:{server.server_port}", "worker-2", 0.05),
        )
        try:
            worker_one.start()
            worker_two.start()
            self._wait_until(lambda: len(state.nodes) == 2, state)

            with httpx_client(f"http://127.0.0.1:{server.server_port}") as client:
                response = client.post(
                    "/api/jobs/submit",
                    json={
                        "title": "Worker Failure Integration Test",
                        "prompts": [f"failure-task-{index}" for index in range(TASK_COUNT)],
                    },
                )
                self.assertEqual(response.status_code, 200)

            self._wait_until(
                lambda: any(node_id == "worker-1" for node_id, _ in state.claim_history),
                state,
            )
            worker_one.terminate()
            worker_one.join(timeout=2)
            self.assertFalse(worker_one.is_alive())

            self._wait_until(lambda: len(state.requeued_chunk_ids) >= 1, state, timeout=5)
            requeued_chunks = set(state.requeued_chunk_ids)
            self.assertTrue(requeued_chunks)

            self._wait_until(
                lambda: state.job_status()["completed_tasks"] == TASK_COUNT,
                state,
                timeout=10,
            )
            worker_two_tasks = set(state.completed_by_node.get("worker-2", []))
            requeued_task_ids = {
                chunk["task"]["task_id"]
                for chunk in state.chunks
                if chunk["chunk_id"] in requeued_chunks
            }
            self.assertTrue(requeued_task_ids & worker_two_tasks)
        finally:
            if worker_one.is_alive():
                worker_one.terminate()
            if worker_two.is_alive():
                worker_two.terminate()
            worker_one.join(timeout=2)
            worker_two.join(timeout=2)
            state.stop()
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1)

    @staticmethod
    def _wait_until(predicate, state: FailureOrchestratorState, timeout: float = 5) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with state.lock:
                if predicate():
                    return
            time.sleep(0.05)
        raise AssertionError("timed out waiting for orchestrator failure-state transition")


class httpx_client:
    def __init__(self, base_url: str):
        import httpx

        self.client = httpx.Client(base_url=base_url, timeout=2)

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.close()


if __name__ == "__main__":
    unittest.main()
