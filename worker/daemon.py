import asyncio
import logging

from .config import WorkerConfig
from .connection import OrchestratorConnection
from .executor import TaskExecutor
from .backoff import ExponentialBackoff
from .ollama_runtime import OllamaRuntime
from .worker import Worker


logger = logging.getLogger(__name__)


class WorkerDaemon:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.stop_event = asyncio.Event()
        self.backoff = ExponentialBackoff(
            config.reconnect_initial,
            config.reconnect_maximum,
            config.reconnect_multiplier,
        )

    def stop(self) -> None:
        self.stop_event.set()

    async def run_forever(self) -> None:
        while not self.stop_event.is_set():
            connection = OrchestratorConnection(self.config)
            runtime = OllamaRuntime(self.config)
            worker: Worker | None = None
            try:
                worker = Worker(
                    self.config,
                    connection,
                    TaskExecutor(self.config.node_id, self.config.ollama_model, runtime),
                )
                await runtime.startup_check()
                await worker.run_session(self.stop_event)
                self.backoff.reset()
            except asyncio.CancelledError:
                raise
            except Exception:
                if worker is not None and worker.registered:
                    self.backoff.reset()
                if worker is not None:
                    worker.mark_disconnected()
                logger.exception("Worker session stopped; reconnecting")
            finally:
                await runtime.close()
                await connection.close()

            if not self.stop_event.is_set():
                delay = self.backoff.next_delay()
                logger.info(
                    "Waiting %.1f seconds before reconnecting worker %s",
                    delay,
                    self.config.node_id,
                )
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=delay,
                    )
                except asyncio.TimeoutError:
                    pass
