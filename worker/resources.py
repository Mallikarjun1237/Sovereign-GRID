from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import platform
import subprocess
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerResources:
    hostname: str
    cores: int
    ram_gb: float
    device_type: str
    gpu_name: str | None = None
    vram_gb: float | None = None


@dataclass(frozen=True)
class ResourceSnapshot:
    worker_id: str
    hostname: str
    cpu_logical_cores: int
    cpu_usage_percent: float
    ram_total_gb: float
    ram_available_gb: float
    ram_usage_percent: float
    gpu_available: bool
    gpu_name: str | None
    vram_total_gb: float | None
    vram_available_gb: float | None
    worker_status: str
    current_task_id: str | None
    timestamp: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "cpu_logical_cores": self.cpu_logical_cores,
            "cpu_usage_percent": self.cpu_usage_percent,
            "ram_total_gb": self.ram_total_gb,
            "ram_available_gb": self.ram_available_gb,
            "ram_usage_percent": self.ram_usage_percent,
            "gpu_available": self.gpu_available,
            "gpu_name": self.gpu_name,
            "vram_total_gb": self.vram_total_gb,
            "vram_available_gb": self.vram_available_gb,
            "worker_status": self.worker_status,
            "current_task_id": self.current_task_id,
            "timestamp": self.timestamp.isoformat(),
        }


def _gpu_details() -> tuple[str | None, float | None, float | None]:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,memory.free",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return None, None, None

    first_line = result.stdout.strip().splitlines()
    if not first_line:
        return None, None, None

    values = [value.strip() for value in first_line[0].split(",")]
    if len(values) < 3:
        logger.warning("GPU query returned an unexpected format: %s", first_line[0])
        return None, None, None

    name, total_memory, available_memory = values[:3]
    try:
        vram_total_gb = round(float(total_memory) / 1024, 2)
        vram_available_gb = round(float(available_memory) / 1024, 2)
    except ValueError:
        return name or None, None, None
    return name or None, vram_total_gb, vram_available_gb


def detect_resources(device_type: str) -> WorkerResources:
    cores = os.cpu_count() or 1
    ram_gb = 0.0
    if psutil is not None:
        ram_gb = round(psutil.virtual_memory().total / (1024**3), 2)
    else:
        logger.warning("psutil is unavailable; reporting 0 GB RAM")

    gpu_name, vram_gb, _ = _gpu_details()
    return WorkerResources(
        hostname=platform.node() or "unknown-host",
        cores=cores,
        ram_gb=ram_gb,
        device_type=device_type,
        gpu_name=gpu_name,
        vram_gb=vram_gb,
    )


class ResourceMonitor:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.hostname = platform.node() or "unknown-host"
        self._prime_cpu_percent()

    @staticmethod
    def _prime_cpu_percent() -> None:
        if psutil is not None:
            psutil.cpu_percent(interval=None)

    def collect(self, worker_status: str, current_task_id: str | None) -> ResourceSnapshot:
        cores = os.cpu_count() or 1
        cpu_usage_percent = 0.0
        ram_total_gb = 0.0
        ram_available_gb = 0.0
        ram_usage_percent = 0.0

        if psutil is None:
            logger.warning("psutil is unavailable; resource usage is reported as zero")
        else:
            try:
                cpu_usage_percent = round(psutil.cpu_percent(interval=None), 2)
                memory = psutil.virtual_memory()
                ram_total_gb = round(memory.total / (1024**3), 2)
                ram_available_gb = round(memory.available / (1024**3), 2)
                ram_usage_percent = round(memory.percent, 2)
            except (OSError, RuntimeError) as exc:
                logger.warning("Could not collect CPU/RAM usage: %s", exc)

        try:
            gpu_name, vram_total_gb, vram_available_gb = _gpu_details()
        except Exception:
            logger.exception("GPU resource detection failed; continuing without GPU metrics")
            gpu_name, vram_total_gb, vram_available_gb = None, None, None

        return ResourceSnapshot(
            worker_id=self.worker_id,
            hostname=self.hostname,
            cpu_logical_cores=cores,
            cpu_usage_percent=cpu_usage_percent,
            ram_total_gb=ram_total_gb,
            ram_available_gb=ram_available_gb,
            ram_usage_percent=ram_usage_percent,
            gpu_available=gpu_name is not None,
            gpu_name=gpu_name,
            vram_total_gb=vram_total_gb,
            vram_available_gb=vram_available_gb,
            worker_status=worker_status,
            current_task_id=current_task_id,
            timestamp=datetime.now(timezone.utc),
        )
