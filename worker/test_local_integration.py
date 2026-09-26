import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest
from urllib.parse import urlparse

from .config import WorkerConfig
from .connection import OrchestratorConnection
from .executor import TaskExecutor
from .runtime import InferenceResult
from .worker import Worker


TASK_ID = "local-integration-task-001"
CHUNK_ID = "local-integration-chunk-001"
JOB_ID = "local-integration-job-001"


class ContractState:
    def __init__(self):
        self.lock = threading.Lock()
        self.registration: dict | None = None
        self.heartbeats = 0
        self.polls = 0
        self.completion: dict | None = None
        self.task_claimed = False


class ContractHandler(BaseHTTPRequestHandler):
    state: ContractState
    server_version = "SovereignGRIDTestOrchestrator/1.0"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(content_length))

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        payload = self._read_json()
        if path == "/api/nodes/register":
            assert set(payload) == {"node_id", "specs"}
            assert set(payload["specs"]) == {"cores", "ram_gb", "device_type"}
            with self.state.lock:
                self.state.registration = payload
            self._send_json(
                200,
                {
                    "node_id": payload["node_id"],
                    "device_type": payload["specs"]["device_type"],
                    "cores": payload["specs"]["cores"],
                    "ram_gb": payload["specs"]["ram_gb"],
                    "status": "ONLINE",
                    "last_heartbeat": "2026-09-26T00:00:00+00:00",
                },
            )
            return
        if path == "/api/nodes/heartbeat":
            assert set(payload) == {"node_id"}
            with self.state.lock:
                self.state.heartbeats += 1
            self._send_json(200, {"status": "alive"})
            return
        if path == "/api/tasks/complete":
            assert set(payload) == {"chunk_id", "node_id", "results"}
            assert payload["chunk_id"] == CHUNK_ID
            assert isinstance(payload["results"], list)
            with self.state.lock:
                self.state.completion = payload
            self._send_json(200, {"status": "success", "completed_count": 1})
            return
        self._send_json(404, {"detail": "not found"})

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path != "/api/tasks/poll":
            self._send_json(404, {"detail": "not found"})
            return
        with self.state.lock:
            self.state.polls += 1
            if self.state.task_claimed:
                payload = None
            else:
                self.state.task_claimed = True
                payload = {
                    "chunk_id": CHUNK_ID,
                    "job_id": JOB_ID,
                    "tasks": [{"task_id": TASK_ID, "prompt": "deterministic input"}],
                }
        self._send_json(200, payload)


class DeterministicRuntime:
    async def execute(self, prompt: str) -> InferenceResult:
        await asyncio.sleep(0.05)
        return InferenceResult(
            output=f"deterministic:{prompt}",
            model="local-test-runtime",
            duration_ms=50.0,
        )


class LocalWorkerIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_existing_http_contract_end_to_end_without_ollama(self) -> None:
        state = ContractState()
        handler = type("BoundContractHandler", (ContractHandler,), {"state": state})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        worker_id = "local-integration-worker-001"
        config = WorkerConfig(
            orchestrator_url=f"http://127.0.0.1:{server.server_port}",
            node_id=worker_id,
            heartbeat_interval=0.02,
            poll_interval=0.01,
            resource_interval=0.02,
            request_timeout=2,
        )
        connection = OrchestratorConnection(config)
        worker = Worker(
            config,
            connection,
            TaskExecutor(worker_id, "local-test-runtime", DeterministicRuntime()),
        )
        stop_event = asyncio.Event()
        worker_task = asyncio.create_task(worker.run_session(stop_event))

        try:
            for _ in range(100):
                await asyncio.sleep(0.01)
                with state.lock:
                    if state.completion is not None:
                        break
            else:
                self.fail("worker did not submit a completion within one second")

            stop_event.set()
            await asyncio.wait_for(worker_task, timeout=1)

            with state.lock:
                registration = state.registration
                completion = state.completion
                heartbeat_count = state.heartbeats
                poll_count = state.polls

            self.assertIsNotNone(registration)
            self.assertEqual(registration["node_id"], worker_id)
            self.assertGreater(registration["specs"]["cores"], 0)
            self.assertGreater(registration["specs"]["ram_gb"], 0)
            self.assertGreaterEqual(heartbeat_count, 1)
            self.assertGreaterEqual(poll_count, 1)
            self.assertIsNotNone(completion)
            self.assertEqual(completion["node_id"], worker_id)
            self.assertEqual(len(completion["results"]), 1)
            self.assertEqual(completion["results"][0]["task_id"], TASK_ID)

            result = json.loads(completion["results"][0]["output"])
            self.assertEqual(result["worker_id"], worker_id)
            self.assertEqual(result["task_id"], TASK_ID)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["output"], "deterministic:deterministic input")
            self.assertLessEqual(
                result["timing"]["task_received_at"],
                result["timing"]["execution_started_at"],
            )
            self.assertLessEqual(
                result["timing"]["execution_started_at"],
                result["timing"]["task_completed_at"],
            )
            self.assertGreaterEqual(result["timing"]["inference_duration_ms"], 0)
            self.assertGreaterEqual(result["timing"]["total_processing_duration_ms"], 0)
            self.assertEqual(result["worker_metrics"]["tasks_received"], 1)
            self.assertEqual(result["worker_metrics"]["successful_tasks"], 1)
            self.assertEqual(result["worker_metrics"]["failed_tasks"], 0)
            self.assertEqual(result["worker_metrics"]["execution_state"], "IDLE")
            self.assertIsInstance(result["worker_metrics"]["cpu_usage_percent"], (int, float))
            self.assertIsInstance(result["worker_metrics"]["ram_usage_percent"], (int, float))
        finally:
            if not worker_task.done():
                stop_event.set()
                await asyncio.wait_for(worker_task, timeout=1)
            await connection.close()
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
