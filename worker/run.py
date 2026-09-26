import asyncio
import logging
import signal

from .config import WorkerConfig
from .daemon import WorkerDaemon


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def main() -> int:
    configure_logging()
    try:
        config = WorkerConfig()
    except ValueError as exc:
        logging.getLogger(__name__).error("Worker configuration error: %s", exc)
        return 2

    daemon = WorkerDaemon(config)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, daemon.stop)
        except NotImplementedError:
            pass
    await daemon.run_forever()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
