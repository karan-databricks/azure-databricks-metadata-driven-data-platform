# tests/retry/test_http_client.py

from unittest.mock import Mock, patch

import pytest
import requests

from enterprise_api_ingestion_platform.common.exceptions import (
    ApiRequestException,
)
from enterprise_api_ingestion_platform.retry.retry_policy import (
    RetryPolicy,
)
from enterprise_api_ingestion_platform.utils.http_client import (
    HttpClient,
)


def create_response(
    status_code: int,
    content_type: str = "application/json",
    json_data: object | None = None,
    text: str = "",
) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 400
    response.headers = {"Content-Type": content_type}
    response.text = text

    if json_data is not None:
        response.json.return_value = json_data

    if response.ok:
        response.raise_for_status.return_value = None
    else:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Client Error",
            response=response,
        )

    return response


@pytest.mark.parametrize(
    "status_code",
    [408, 429, 500, 502, 503, 504],
)
def test_retryable_http_status_is_retried(
    status_code: int,
) -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_delay_seconds=1.0,
        backoff_factor=2.0,
    )
    client = HttpClient(retry_policy=policy)

    failed_response = create_response(status_code=status_code)
    successful_response = create_response(
        status_code=200,
        json_data={"success": True},
    )

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=[
                failed_response,
                successful_response,
            ],
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        result = client.request(
            method="GET",
            url="https://example.com",
        )

    assert result == {"success": True}
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once_with(1.0)
    failed_response.close.assert_called_once()


@pytest.mark.parametrize(
    "status_code",
    [400, 401, 403, 404, 422],
)
def test_non_retryable_http_status_fails_immediately(
    status_code: int,
) -> None:
    policy = RetryPolicy(max_retries=3)
    client = HttpClient(retry_policy=policy)

    response = create_response(status_code=status_code)

    with patch(
        "enterprise_api_ingestion_platform.utils.http_client.requests.request",
        return_value=response,
    ) as mock_request:
        with pytest.raises(ApiRequestException):
            client.request(
                method="GET",
                url="https://example.com",
            )

    assert mock_request.call_count == 1
    response.close.assert_called_once()


def test_retryable_status_exhausts_retries() -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_delay_seconds=1.0,
        backoff_factor=2.0,
    )
    client = HttpClient(retry_policy=policy)

    responses = [
        create_response(status_code=503),
        create_response(status_code=503),
        create_response(status_code=503),
        create_response(status_code=503),
    ]

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=responses,
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        with pytest.raises(ApiRequestException):
            client.request(
                method="GET",
                url="https://example.com",
            )

    assert mock_request.call_count == 4
    assert mock_sleep.call_count == 3
    assert [
        call.args[0]
        for call in mock_sleep.call_args_list
    ] == [1.0, 2.0, 4.0]


def test_timeout_is_retried() -> None:
    policy = RetryPolicy(
        max_retries=2,
        initial_delay_seconds=1.0,
    )
    client = HttpClient(retry_policy=policy)

    successful_response = create_response(
        status_code=200,
        json_data={"success": True},
    )

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=[
                requests.Timeout("request timed out"),
                successful_response,
            ],
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        result = client.request(
            method="GET",
            url="https://example.com",
        )

    assert result == {"success": True}
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


def test_connection_error_is_retried() -> None:
    policy = RetryPolicy(
        max_retries=2,
        initial_delay_seconds=1.0,
    )
    client = HttpClient(retry_policy=policy)

    successful_response = create_response(
        status_code=200,
        json_data={"success": True},
    )

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=[
                requests.ConnectionError("connection failed"),
                successful_response,
            ],
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        result = client.request(
            method="GET",
            url="https://example.com",
        )

    assert result == {"success": True}
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


def test_timeout_exhausts_retries() -> None:
    policy = RetryPolicy(
        max_retries=2,
        initial_delay_seconds=1.0,
        backoff_factor=2.0,
    )
    client = HttpClient(retry_policy=policy)

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=requests.Timeout("request timed out"),
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        with pytest.raises(ApiRequestException) as exc_info:
            client.request(
                method="GET",
                url="https://example.com",
            )

    assert "request timed out" in str(exc_info.value)
    assert mock_request.call_count == 3
    assert [
        call.args[0]
        for call in mock_sleep.call_args_list
    ] == [1.0, 2.0]


def test_stream_uses_retry_logic() -> None:
    policy = RetryPolicy(
        max_retries=1,
        initial_delay_seconds=1.0,
    )
    client = HttpClient(retry_policy=policy)

    failed_response = create_response(status_code=503)
    successful_response = create_response(status_code=200)
    successful_response.iter_content.return_value = [
        b"first chunk",
        b"second chunk",
    ]

    with (
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.requests.request",
            side_effect=[
                failed_response,
                successful_response,
            ],
        ) as mock_request,
        patch(
            "enterprise_api_ingestion_platform.utils.http_client.time.sleep",
        ) as mock_sleep,
    ):
        chunks = list(
            client.stream(
                method="GET",
                url="https://example.com",
            )
        )

    assert chunks == [
        b"first chunk",
        b"second chunk",
    ]
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once_with(1.0)