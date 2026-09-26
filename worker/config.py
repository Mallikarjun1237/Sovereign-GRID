from dataclasses import dataclass, field
import os
from pathlib import Path
import socket
from urllib.parse import urlparse
import uuid

from dotenv import load_dotenv


load_dotenv(Path(__file__).resolve().parent / ".env")


def _worker_id() -> str:
    configured = os.getenv("WORKER_NODE_ID")
    if configured:
        return configured

    id_path = Path(
        os.getenv(
            "WORKER_ID_FILE",
            str(Path(__file__).resolve().parent / ".worker_id"),
        )
    )
    try:
        existing = id_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        existing = ""
    except OSError as exc:
        raise ValueError(f"Cannot read WORKER_ID_FILE '{id_path}': {exc}") from exc
    if existing:
        return existing

    generated = f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
    try:
        id_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = id_path.with_name(f"{id_path.name}.tmp")
        temporary_path.write_text(generated + "\n", encoding="utf-8")
        os.replace(temporary_path, id_path)
    except OSError as exc:
        raise ValueError(
            f"Cannot persist worker ID to '{id_path}'. Set WORKER_ID_FILE to a writable path: {exc}"
        ) from exc
    return generated


def _orchestrator_url() -> str:
    configured_url = os.getenv("ORCHESTRATOR_URL", "").strip()
    if configured_url:
        return configured_url.rstrip("/")

    host = os.getenv("ORCHESTRATOR_HOST", "").strip()
    if not host:
        raise ValueError(
            "Set ORCHESTRATOR_URL or ORCHESTRATOR_HOST in worker/.env; "
            "the worker has no localhost fallback for LAN deployment"
        )
    port = os.getenv("ORCHESTRATOR_PORT", "8000").strip()
    return f"http://{host}:{port}".rstrip("/")


def _validate_local_ollama_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError(
            "OLLAMA_URL must point to a local Ollama instance using http://localhost, "
            "http://127.0.0.1, or http://[::1]"
        )


@dataclass(frozen=True)
class WorkerConfig:
    orchestrator_url: str = field(default_factory=_orchestrator_url)
    node_id: str = field(default_factory=_worker_id)
    device_type: str = os.getenv("WORKER_DEVICE_TYPE", "cpu")
    poll_interval: float = float(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "2"))
    heartbeat_interval: float = float(os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "3"))
    resource_interval: float = float(os.getenv("WORKER_RESOURCE_INTERVAL_SECONDS", "5"))
    reconnect_initial: float = float(os.getenv("WORKER_RECONNECT_INITIAL_SECONDS", "2"))
    reconnect_maximum: float = float(os.getenv("WORKER_RECONNECT_MAXIMUM_SECONDS", "60"))
    reconnect_multiplier: float = float(os.getenv("WORKER_RECONNECT_MULTIPLIER", "2"))
    request_timeout: float = float(os.getenv("WORKER_REQUEST_TIMEOUT_SECONDS", "15"))
    ollama_url: str = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "tinyllama:latest")
    ollama_timeout: float = float(os.getenv("OLLAMA_INFERENCE_TIMEOUT_SECONDS", "300"))
    ollama_keep_alive: str = os.getenv("OLLAMA_KEEP_ALIVE", "5m")

    def __post_init__(self) -> None:
        _validate_local_ollama_url(self.ollama_url)
