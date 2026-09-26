from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class InferenceResult:
    output: str
    model: str
    duration_ms: float


class InferenceRuntime(Protocol):
    async def startup_check(self) -> None:
        ...

    async def execute(self, prompt: str) -> InferenceResult:
        ...

    async def close(self) -> None:
        ...
