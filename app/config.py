import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    database_url: str
    # Simulated external API latency for each transaction.
    processing_delay_seconds: float
    # How long a worker "owns" a transaction before another worker may retry it.
    # Must comfortably exceed the processing delay.
    lease_seconds: float
    # How often the recovery sweeper looks for unfinished transactions.
    sweep_interval_seconds: float
    # Backoff before a failed attempt becomes eligible for retry.
    retry_backoff_seconds: float
    # Separate connection pools so background processing can never starve the
    # webhook endpoint of connections (keeps acknowledgements fast under load).
    api_pool_size: int
    worker_pool_size: int


def get_settings() -> Settings:
    delay = _float_env("PROCESSING_DELAY_SECONDS", 30.0)
    return Settings(
        database_url=os.getenv("DATABASE_URL", ""),
        processing_delay_seconds=delay,
        lease_seconds=_float_env("LEASE_SECONDS", max(delay * 2, delay + 30.0)),
        sweep_interval_seconds=_float_env("SWEEP_INTERVAL_SECONDS", 10.0),
        retry_backoff_seconds=_float_env("RETRY_BACKOFF_SECONDS", 5.0),
        api_pool_size=int(_float_env("API_POOL_SIZE", 15)),
        worker_pool_size=int(_float_env("WORKER_POOL_SIZE", 5)),
    )
