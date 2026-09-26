import argparse
import asyncio
from dataclasses import replace
import logging
import sys
from typing import Any

import httpx

from .config import WorkerConfig
from .connection import OrchestratorConnection
from .executor import TaskExecutor
from .ollama_runtime import (
    InferenceTimeoutError,
    ModelNotInstalledError,
    OllamaConnectionError,
    OllamaRuntime,
    OllamaRuntimeError,
)
from .resources import detect_resources
from .worker import Worker


logger = logging.getLogger(__name__)


class IntegrationFailure(RuntimeError):
    pass


async def submit_test_job(connection: OrchestratorConnection, prompt: str) -> str:
    try:
        response = await connection.client.post(
            "/api/jobs/submit",
            json={"title": "Worker Ollama Integration Test", "prompts": [prompt]},
        )
        response.raise_for_status()
        payload: Any = response.json()
    except httpx.ConnectError as exc:
        raise IntegrationFailure(
            f"Cannot reach orchestrator at {connection.config.orchestrator_url}. "
            "Start it with 'python orchestrator/run.py' and verify PostgreSQL is running."
        ) from exc
    except httpx.HTTPError as exc:
        raise IntegrationFailure(f"Orchestrator rejected test job: {exc}") from exc

    job_id = payload.get("id") if isinstance(payload, dict) else None
    if not isinstance(job_id, str) or not job_id:
        raise IntegrationFailure("Orchestrator returned no job ID for the integration task")
    logger.info("Submitted integration job %s", job_id)
    return job_id


async def wait_for_completion(
    connection: OrchestratorConnection,
    job_id: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        try:
            response = await connection.client.get(f"/api/jobs/{job_id}")
            response.raise_for_status()
            payload: Any = response.json()
        except httpx.HTTPError as exc:
            raise IntegrationFailure(f"Could not read job {job_id} from orchestrator: {exc}") from exc

        if not isinstance(payload, dict):
            raise IntegrationFailure("Orchestrator returned an invalid job status payload")
        status = payload.get("status")
        if status == "COMPLETED":
            return payload
        if status == "FAILED":
            raise IntegrationFailure(f"Orchestrator marked integration job {job_id} as FAILED")
        await asyncio.sleep(0.25)
    raise IntegrationFailure(
        f"Timed out waiting for orchestrator job {job_id} after {timeout_seconds:.1f} seconds"
    )


def actionable_message(exc: Exception) -> str:
    if isinstance(exc, OllamaConnectionError):
        return (
            f"{exc} Start Ollama locally, then verify it with 'ollama list'. "
            "No cloud API is used."
        )
    if isinstance(exc, ModelNotInstalledError):
        return f"{exc} Install it manually; this test never downloads models."
    if isinstance(exc, InferenceTimeoutError):
        return f"{exc} Increase OLLAMA_INFERENCE_TIMEOUT_SECONDS or use a smaller local model."
    if isinstance(exc, OllamaRuntimeError):
        return f"Local Ollama execution failed: {exc}"
    if isinstance(exc, IntegrationFailure):
        return str(exc)
    if isinstance(exc, httpx.TimeoutException):
        return (
            "The orchestrator request timed out. Confirm it is running, verify PostgreSQL, "
            "and increase WORKER_REQUEST_TIMEOUT_SECONDS if the LAN is slow."
        )
    if isinstance(exc, httpx.ConnectError):
        return (
            "Cannot reach the orchestrator. Start it with 'python orchestrator/run.py', "
            "verify PostgreSQL, and confirm ORCHESTRATOR_URL."
        )
    return f"Integration test failed: {type(exc).__name__}: {exc}"


async def run_integration(config: WorkerConfig, prompt: str, timeout_seconds: float) -> dict[str, Any]:
    runtime = OllamaRuntime(config)
    connection = OrchestratorConnection(config)
    worker_task: asyncio.Task[None] | None = None
    stop_event = asyncio.Event()
    try:
        await runtime.startup_check()
        resources = detect_resources(config.device_type)
        await connection.register(resources)
        job_id = await submit_test_job(connection, prompt)

        worker = Worker(
            config,
            connection,
            TaskExecutor(config.node_id, config.ollama_model, runtime),
        )
        logger.info("Starting worker %s for Ollama integration job", config.node_id)
        worker_task = asyncio.create_task(worker.run_session(stop_event))
        job = await wait_for_completion(connection, job_id, timeout_seconds)
        if job.get("completed_tasks") != job.get("total_tasks"):
            raise IntegrationFailure(f"Job {job_id} completed with inconsistent task counts: {job}")
        return job
    finally:
        stop_event.set()
        if worker_task is not None:
            try:
                await asyncio.wait_for(worker_task, timeout=2)
            except asyncio.TimeoutError:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
            except Exception:
                logger.debug("Worker session ended during integration cleanup", exc_info=True)
        await runtime.close()
        await connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a real local Ollama worker integration test")
    parser.add_argument("--orchestrator-url", default=None)
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument(
        "--prompt",
        default="Reply with exactly one short sentence confirming local worker inference.",
    )
    parser.add_argument("--timeout", type=float, default=180.0)
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    try:
        config = WorkerConfig()
        overrides: dict[str, str] = {}
        if args.orchestrator_url:
            overrides["orchestrator_url"] = args.orchestrator_url.rstrip("/")
        if args.worker_id:
            overrides["node_id"] = args.worker_id
        if args.model:
            overrides["ollama_model"] = args.model
        config = replace(config, **overrides)
        job = await run_integration(config, args.prompt, args.timeout)
    except Exception as exc:
        print(f"ERROR: {actionable_message(exc)}", file=sys.stderr)
        return 2
    print(
        "SUCCESS: local Ollama inference reached the orchestrator "
        f"(job_id={job['id']}, completed_tasks={job['completed_tasks']}, "
        f"model={config.ollama_model})"
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(asyncio.run(main()))
