from dataclasses import dataclass


@dataclass
class ExponentialBackoff:
    initial_seconds: float
    maximum_seconds: float
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0:
            raise ValueError("initial_seconds must be positive")
        if self.maximum_seconds < self.initial_seconds:
            raise ValueError("maximum_seconds must be at least initial_seconds")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        self._attempt = 0

    def next_delay(self) -> float:
        delay = min(
            self.initial_seconds * (self.multiplier**self._attempt),
            self.maximum_seconds,
        )
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0