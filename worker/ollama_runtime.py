import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any

import httpx

from .config import WorkerConfig
from .runtime import InferenceResult


logger = logging.getLogger(__name__)


class OllamaRuntimeError(RuntimeError):
    """Base error for local Ollama runtime failures."""

    safe_for_response = True


class OllamaConnectionError(OllamaRuntimeError):
    """Ollama is unavailable or cannot be reached."""


class ModelNotInstalledError(OllamaRuntimeError):
    """The configured model is not available in the local Ollama registry."""


class InferenceTimeoutError(OllamaRuntimeError):
    """Ollama did not finish inference before the configured timeout."""


@dataclass(frozen=True)
class OllamaModel:
    name: str


class OllamaRuntime:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            base_url=config.ollama_url,
            timeout=httpx.Timeout(config.ollama_timeout),
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def list_models(self) -> list[OllamaModel]:
        try:
            response = await self.client.get("/api/tags")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.NetworkError) as exc:
            raise OllamaConnectionError(
                f"Cannot connect to Ollama at {self.config.ollama_url}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise OllamaConnectionError("Timed out while checking Ollama availability") from exc
        except httpx.HTTPError as exc:
            raise OllamaRuntimeError(f"Ollama model listing failed: {exc}") from exc

        payload: Any = response.json()
        raw_models = payload.get("models") if isinstance(payload, dict) else None
        if not isinstance(raw_models, list):
            raise OllamaRuntimeError("Ollama model listing did not contain a 'models' list")

        models: list[OllamaModel] = []
        for raw_model in raw_models:
            if isinstance(raw_model, dict) and isinstance(raw_model.get("name"), str):
                models.append(OllamaModel(name=raw_model["name"]))
        return models

    async def startup_check(self) -> None:
        models = await self.list_models()
        available_names = {model.name for model in models}
        if self.config.ollama_model not in available_names:
            installed = ", ".join(sorted(available_names)) or "none"
            raise ModelNotInstalledError(
                f"Ollama model '{self.config.ollama_model}' is not installed. "
                f"Installed models: {installed}. Run 'ollama pull {self.config.ollama_model}'."
            )
        logger.info("Ollama ready with selected model %s", self.config.ollama_model)

    async def execute(self, prompt: str) -> InferenceResult:
        started_at = time.perf_counter()
        try:
            async with asyncio.timeout(self.config.ollama_timeout):
                response = await self.client.post(
                    "/api/generate",
                    json={
                        "model": self.config.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "keep_alive": self.config.ollama_keep_alive,
                    },
                )
                if response.status_code == 404:
                    raise ModelNotInstalledError(
                        f"Ollama model '{self.config.ollama_model}' is not installed. "
                        f"Run 'ollama pull {self.config.ollama_model}'."
                    )
                response.raise_for_status()
                payload: Any = response.json()
        except ModelNotInstalledError:
            raise
        except asyncio.TimeoutError as exc:
            raise InferenceTimeoutError(
                f"Ollama inference exceeded {self.config.ollama_timeout:.1f} seconds"
            ) from exc
        except (httpx.ConnectError, httpx.NetworkError) as exc:
            raise OllamaConnectionError(
                f"Cannot connect to Ollama at {self.config.ollama_url}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise InferenceTimeoutError(
                f"Ollama inference exceeded {self.config.ollama_timeout:.1f} seconds"
            ) from exc
        except httpx.HTTPError as exc:
            raise OllamaRuntimeError(f"Ollama inference request failed: {exc}") from exc

        output = payload.get("response") if isinstance(payload, dict) else None
        if not isinstance(output, str):
            raise OllamaRuntimeError("Ollama response did not contain a string 'response' field")

        return InferenceResult(
            output=output,
            model=self.config.ollama_model,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 2),
        )
