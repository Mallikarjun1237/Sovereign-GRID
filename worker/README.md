# Sovereign-GRID Worker

This is an independently executable Worker/AI Runtime for the existing orchestrator. It uses the existing HTTP contract without importing or modifying the orchestrator package.

## Requirements

- Python 3.11 or newer
- A reachable Sovereign-GRID orchestrator
- Ollama running locally on the worker laptop
- The selected model already pulled into Ollama

Install dependencies from the repository root:

```powershell
python -m pip install -r worker/requirements.txt
```

## LAN deployment

Use a private LAN with one machine running the orchestrator and one worker process per worker machine. The orchestrator already binds to `0.0.0.0:8000` in `orchestrator/run.py`; no orchestrator code change is required.

### Machine 1: orchestrator

Find the machine's private IPv4 address:

```powershell
ipconfig
```

Start PostgreSQL, then start the existing backend from the repository root:

```powershell
python -m pip install -r orchestrator/requirements.txt
python orchestrator/run.py
```

Allow inbound TCP port 8000 on a Windows Private network. Run PowerShell as Administrator on Machine 1:

```powershell
New-NetFirewallRule -DisplayName "Sovereign-GRID Orchestrator 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow -Profile Private
```

Verify locally with `Invoke-WebRequest http://localhost:8000/health`. Workers need outbound TCP access to Machine 1 port 8000; they do not need PostgreSQL access.

### Machine 2 and Machine 3: workers

On each worker machine, use a separate checkout or virtual environment. Replace `192.168.1.10` below with Machine 1's private IPv4 address:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r worker/requirements.txt
Copy-Item worker/env.example worker/.env
notepad worker/.env
```

Set these values in `worker/.env` on both machines:

```dotenv
ORCHESTRATOR_URL=
ORCHESTRATOR_HOST=192.168.1.10
ORCHESTRATOR_PORT=8000
WORKER_NODE_ID=
WORKER_ID_FILE=.worker_id
OLLAMA_URL=http://127.0.0.1:11434
OLLAMA_MODEL=tinyllama:latest
```

`ORCHESTRATOR_URL` may instead be set to `http://192.168.1.10:8000`; it takes precedence over host/port. There is no localhost fallback, so a missing LAN address fails with a configuration error rather than silently targeting the worker laptop.

Each worker automatically generates an ID containing its hostname and a random suffix, then persists it in `worker/.worker_id`. Keep Machine 2 and Machine 3 as separate filesystem installations and do not copy that file between them. You may set explicit distinct IDs instead, for example `WORKER_NODE_ID=worker-machine-1` and `WORKER_NODE_ID=worker-machine-2`.

Install and start Ollama locally on each worker. Do not point `OLLAMA_URL` at the orchestrator or a cloud service:

```powershell
ollama pull tinyllama:latest
ollama list
```

Start each worker from the repository root:

```powershell
\.venv\Scripts\Activate.ps1
python -m worker.run
```

Check LAN reachability before starting a worker:

```powershell
Test-NetConnection 192.168.1.10 -Port 8000
```

Workers send only task protocol traffic to the configured orchestrator address. AI prompts and inference requests go to each worker's local Ollama loopback address, so task data stays on the private LAN and is not sent to an external AI API.

## Privacy and network boundary

During normal runtime, the worker creates exactly two HTTP clients:

- **Configured orchestrator:** `POST /api/nodes/register`, `POST /api/nodes/heartbeat`, `GET /api/tasks/poll`, and `POST /api/tasks/complete`. The base URL comes only from `ORCHESTRATOR_URL` or `ORCHESTRATOR_HOST` plus `ORCHESTRATOR_PORT`.
- **Local Ollama:** `GET /api/tags` for model availability and `POST /api/generate` for inference. `OLLAMA_URL` is validated as loopback-only (`localhost`, `127.0.0.1`, or `::1`), so task prompts cannot be sent to a remote Ollama host through normal worker configuration.

The worker has no OpenAI, Gemini, Claude, Anthropic, telemetry, analytics, tracing, WebSocket, or other third-party service integration. It does not make DNS/API calls to any fixed cloud endpoint. `nvidia-smi` is invoked locally for optional GPU metrics and is not a network client.

The `pip install` commands in this document contact the configured Python package index during setup, and `ollama pull` contacts the Ollama model registry when you manually download a model. Those are installation operations, not worker task traffic. The running worker never downloads models and never sends prompts to those services.

Task prompts are not included in worker lifecycle or resource logs. Logs include operational identifiers such as worker ID, task ID, chunk ID, model name, endpoint errors, and timing. Unexpected task exceptions are logged by type only; their raw messages and stack traces are not returned to the orchestrator.

## Local integration test

Before testing Ollama, run the deterministic HTTP integration test from the repository root:

```powershell
python -m unittest worker.test_local_integration -v
```

The test starts a local contract-compatible orchestrator test server, starts the real worker session and `OrchestratorConnection`, registers a worker, observes resource-backed registration data and heartbeats, delivers a mock task, executes it through a deterministic local runtime, validates the serialized result, and accepts the completion. It does not invoke Ollama or modify the production orchestrator.

To exercise worker failure handling before using Ollama:

```powershell
python -m unittest worker.test_worker_failure_integration -v
```

This test starts two real worker processes and a contract-compatible orchestrator test server, submits four tasks, forcefully terminates Worker 1 during a deliberately slow deterministic task, waits for the server-side heartbeat watcher to mark it dead and requeue its in-progress chunk, and verifies Worker 2 completes the requeued work. Requeue behavior exists only in the orchestrator-side test harness, mirroring the production heartbeat watcher; the worker contains no task reassignment logic. The production orchestrator could not be run in the current environment because PostgreSQL is unavailable, so this test is not a substitute for a database-backed production run.

## Real Ollama integration test

After the deterministic test passes and the real orchestrator is running, run:

```powershell
python -m worker.test_ollama_integration
```

This test checks the local Ollama service and configured model, registers the worker, submits one small job through `/api/jobs/submit`, starts the real worker session, executes inference through local Ollama, and waits for `/api/jobs/{job_id}` to report all tasks completed. It records inference timing in the worker result envelope and confirms the orchestrator accepted the completion through its completed-task count.

Useful overrides:

```powershell
python -m worker.test_ollama_integration --orchestrator-url http://192.168.1.20:8000 --model tinyllama:latest
```

If Ollama or the orchestrator is unavailable, the script exits with status `2` and prints a concrete action such as starting Ollama, running `ollama list`, pulling the selected model manually, or starting the orchestrator. It never downloads a model and never calls an external AI API.

The daemon first checks Ollama's local `/api/tags` endpoint and refuses to start task processing when the selected model is unavailable. It then registers with `/api/nodes/register`, sends heartbeats to `/api/nodes/heartbeat`, polls `/api/tasks/poll`, executes prompts through Ollama's local `/api/generate`, and submits complete chunks to `/api/tasks/complete`.

## Connection management

The worker uses one stable `WORKER_NODE_ID` for its entire daemon lifetime. When the ID is not configured, it is generated once when `WorkerConfig` is created and reused for every reconnect, so reconnects re-register the same node instead of creating duplicate identities.

After registration, the worker maintains the existing heartbeat loop and task polling loop. A failed heartbeat, poll, or completion request ends the current session; the daemon closes the HTTP client, marks the worker disconnected in its logs, and creates a fresh session that registers the same ID again. Reconnection uses bounded exponential backoff:

- `WORKER_RECONNECT_INITIAL_SECONDS`: first delay, default `2`
- `WORKER_RECONNECT_MAXIMUM_SECONDS`: maximum delay, default `60`
- `WORKER_RECONNECT_MULTIPLIER`: growth factor, default `2`

Shutdown interrupts both active sessions and backoff waits. The worker never reassigns, retries, or moves a task to another worker. If a disconnect occurs while a task or completion request is in flight, the worker stops that session without submitting a fabricated result; the orchestrator heartbeat watcher remains responsible for deciding whether the task chunk must be re-queued.

## Resource monitoring

The worker collects a structured resource snapshot every `WORKER_RESOURCE_INTERVAL_SECONDS` seconds, defaulting to 5 seconds. Each snapshot contains:

- worker ID, hostname, timestamp, status, and current task ID
- CPU logical core count and CPU usage percentage
- total RAM, available RAM, and RAM usage percentage
- GPU availability, GPU name, total VRAM, and available VRAM when detectable

CPU and memory metrics use the cross-platform `psutil` library. NVIDIA GPU and VRAM metrics use `nvidia-smi` when it is installed and callable. Missing GPU tools, non-NVIDIA systems, malformed GPU output, and GPU command failures are handled as unavailable metrics; they never stop the worker.

Snapshots are logged as `worker_resource_snapshot` JSON records and are collected alongside heartbeat activity. The current orchestrator heartbeat schema accepts only `node_id`, so the worker preserves that exact request and cannot persist the detailed snapshot in the backend without a new orchestrator API. The supported static registration fields (`cores`, `ram_gb`, and `device_type`) continue to be sent normally.

## Ollama models

The repository does not define a separate model catalog. The worker default is the verified lightweight local tag `tinyllama:latest`, so install it explicitly on another worker laptop before starting the worker:

```powershell
ollama pull tinyllama:latest
ollama list
```

The following local model tags were available during implementation and are supported when installed and selected explicitly:

```powershell
ollama pull mistral:latest
$env:OLLAMA_MODEL = "mistral:latest"
python -m worker.run

ollama pull phi3:mini
$env:OLLAMA_MODEL = "phi3:mini"
python -m worker.run
```

The worker never runs `ollama pull` and never sends inference requests to a cloud provider. Model availability is checked with Ollama's local API before registration.

Inference results are structured internally with the selected model and elapsed milliseconds. The existing orchestrator accepts only the protocol's string `output`, so only that field is sent in task completion requests.

## Performance instrumentation

Each completion envelope sent through the existing `/api/tasks/complete` protocol includes a `timing` object with task receipt time, execution start time, completion time, inference duration, and total processing duration. It also includes `worker_metrics` containing successful and failed task counts, received task count, current execution state, current task ID, CPU usage, RAM usage, available RAM, worker ID, and a metrics timestamp.

The orchestrator stores this JSON envelope in the task's existing string `output` field. No competing dashboard or frontend change is added. Existing dashboard aggregate endpoints do not parse these nested metrics, so detailed metrics are available in stored task output and worker logs but cannot become new dashboard fields without a backend schema/API change.

## Task execution pipeline

Each task follows this sequence:

1. Receive the orchestrator's unchanged `task_id` and `prompt`.
2. Validate both fields without coercing or replacing the task ID.
3. Map the existing prompt-only schema to the default `generate` operation, selected model, and input text.
4. Execute asynchronously through the registered local runtime operation.
5. Capture output, status, error details, and processing time.
6. Serialize the result and submit it with the original task ID.

The submitted `output` remains a string as required by the existing orchestrator. That string contains deterministic JSON with `worker_id`, `task_id`, `status`, `processing_time_ms`, `model`, `output`, and `error`. Successful and failed tasks are both returned, so one failed task does not stop the remaining tasks in its chunk. The current backend stores this envelope as output and does not independently interpret its failure status.

Known validation, timeout, model, and local runtime errors return useful sanitized messages. Unexpected exceptions return a generic error message; stack traces and raw exception details remain in worker logs only.

Task lifecycle logs use the events `task_received`, `task_started`, `task_completed`, and `task_failed`. Additional operations can be registered through `OperationRegistry` without coupling the task executor to Ollama.

## Failure behavior

Network failures cause the worker to reconnect. A task-level model failure is serialized as a failed result envelope and does not stop later tasks in the chunk. Orchestrator connection failures happen outside task serialization and cause the worker session to reconnect.

`OLLAMA_INFERENCE_TIMEOUT_SECONDS` defaults to 300 seconds. Connection failures, timeout failures, malformed responses, and missing models are logged with distinct runtime errors. The daemon reconnects after temporary Ollama or orchestrator failures.
