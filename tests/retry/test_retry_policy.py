# tests/retry/test_retry_policy.py

import pytest

from enterprise_api_ingestion_platform.retry.retry_policy import (
    RetryPolicy,
)


def test_default_policy_configuration() -> None:
    policy = RetryPolicy()

    assert policy.max_retries == 3
    assert policy.initial_delay_seconds == 1.0
    assert policy.backoff_factor == 2.0

    assert 408 in policy.retryable_status_codes
    assert 429 in policy.retryable_status_codes
    assert 500 in policy.retryable_status_codes
    assert 502 in policy.retryable_status_codes
    assert 503 in policy.retryable_status_codes
    assert 504 in policy.retryable_status_codes


@pytest.mark.parametrize(
    "status_code",
    [408, 429, 500, 502, 503, 504],
)
def test_retryable_status_code_is_retried(
    status_code: int,
) -> None:
    policy = RetryPolicy(max_retries=3)

    assert policy.should_retry(
        status_code=status_code,
        attempt=0,
    )
    assert policy.should_retry(
        status_code=status_code,
        attempt=1,
    )
    assert policy.should_retry(
        status_code=status_code,
        attempt=2,
    )


@pytest.mark.parametrize(
    "status_code",
    [200, 400, 401, 403, 404, 422],
)
def test_non_retryable_status_code_is_not_retried(
    status_code: int,
) -> None:
    policy = RetryPolicy(max_retries=3)

    assert not policy.should_retry(
        status_code=status_code,
        attempt=0,
    )


def test_retry_stops_after_max_retries() -> None:
    policy = RetryPolicy(max_retries=3)

    assert policy.should_retry(
        status_code=503,
        attempt=0,
    )
    assert policy.should_retry(
        status_code=503,
        attempt=1,
    )
    assert policy.should_retry(
        status_code=503,
        attempt=2,
    )
    assert not policy.should_retry(
        status_code=503,
        attempt=3,
    )


def test_exponential_backoff() -> None:
    policy = RetryPolicy(
        initial_delay_seconds=1.0,
        backoff_factor=2.0,
    )

    assert policy.get_delay(0) == 1.0
    assert policy.get_delay(1) == 2.0
    assert policy.get_delay(2) == 4.0
    assert policy.get_delay(3) == 8.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": -1},
        {"initial_delay_seconds": 0},
        {"initial_delay_seconds": -1},
        {"backoff_factor": 0},
        {"backoff_factor": 0.5},
        {"retryable_status_codes": frozenset()},
    ],
)
def test_invalid_policy_configuration(
    kwargs: dict,
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)