# src/enterprise_api_ingestion_platform/retry/retry_policy.py

from dataclasses import dataclass, field
from typing import Final


DEFAULT_MAX_RETRIES: Final[int] = 3
DEFAULT_INITIAL_DELAY_SECONDS: Final[float] = 1.0
DEFAULT_BACKOFF_FACTOR: Final[float] = 2.0

DEFAULT_RETRYABLE_STATUS_CODES: Final[frozenset[int]] = frozenset(
    {
        408,
        429,
        500,
        502,
        503,
        504,
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    """
    Configuration for retrying transient HTTP failures.
    """

    max_retries: int = DEFAULT_MAX_RETRIES
    initial_delay_seconds: float = DEFAULT_INITIAL_DELAY_SECONDS
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
    retryable_status_codes: frozenset[int] = field(
        default_factory=lambda: DEFAULT_RETRYABLE_STATUS_CODES,
    )

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(
                "max_retries must be greater than or equal to zero."
            )

        if self.initial_delay_seconds <= 0:
            raise ValueError(
                "initial_delay_seconds must be greater than zero."
            )

        if self.backoff_factor < 1:
            raise ValueError(
                "backoff_factor must be greater than or equal to one."
            )

        if not self.retryable_status_codes:
            raise ValueError(
                "retryable_status_codes must not be empty."
            )

    def should_retry(
        self,
        status_code: int,
        attempt: int,
    ) -> bool:
        """
        Returns True when the HTTP response should be retried.
        """

        return (
            attempt < self.max_retries
            and status_code in self.retryable_status_codes
        )

    def get_delay(
        self,
        attempt: int,
    ) -> float:
        """
        Returns the exponential backoff delay in seconds.

        Attempt zero returns the initial delay.
        """

        return self.initial_delay_seconds * (
            self.backoff_factor ** attempt
        )