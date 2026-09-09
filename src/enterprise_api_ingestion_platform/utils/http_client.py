# src/enterprise_api_ingestion_platform/utils/http_client.py

import time
from typing import Any

import requests

from enterprise_api_ingestion_platform.common.exceptions import (
    ApiRequestException,
)
from enterprise_api_ingestion_platform.retry.retry_policy import (
    RetryPolicy,
)


class HttpClient:
    """
    Generic HTTP client responsible for executing buffered and
    streaming HTTP requests with transient-failure retries.
    """

    DEFAULT_TIMEOUT_SECONDS = 60
    DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024

    def __init__(
        self,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """
        Initializes the HTTP client.

        Parameters
        ----------
        retry_policy:
            Retry configuration. The default policy retries transient
            HTTP failures up to three times using exponential backoff.
        """

        self.retry_policy = retry_policy or RetryPolicy()

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
    ) -> Any:
        """
        Executes a buffered HTTP request.

        JSON responses are returned as Python objects.
        Non-JSON responses are returned as text.
        """

        response = self._send_request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
            data=data,
            stream=False,
        )

        content_type = response.headers.get(
            "Content-Type",
            "",
        ).lower()

        if "application/json" in content_type:
            try:
                return response.json()
            except ValueError:
                return response.text

        return response.text

    def stream(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ):
        """
        Executes a streaming HTTP request.

        Returns
        -------
        Iterator[bytes]
            Response content chunks.
        """

        response = self._send_request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
            data=data,
            stream=True,
        )

        return response.iter_content(
            chunk_size=chunk_size,
        )

    def _send_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None,
        params: dict[str, Any] | None,
        json: Any | None,
        data: Any | None,
        stream: bool,
    ) -> requests.Response:
        """
        Sends the HTTP request with retry handling for transient
        failures.
        """

        for attempt in range(
            self.retry_policy.max_retries + 1,
        ):
            response: requests.Response | None = None

            try:
                response = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                    timeout=self.DEFAULT_TIMEOUT_SECONDS,
                    stream=stream,
                )

                if response.ok:
                    return response

                status_code = response.status_code

                if not self.retry_policy.should_retry(
                    status_code=status_code,
                    attempt=attempt,
                ):
                    response.raise_for_status()

                delay = self.retry_policy.get_delay(
                    attempt=attempt,
                )

                response.close()
                time.sleep(delay)

            except requests.Timeout as exc:
                if not self._should_retry_exception(attempt):
                    raise ApiRequestException(
                        str(exc)
                    ) from exc

                if response is not None:
                    response.close()

                delay = self.retry_policy.get_delay(
                    attempt=attempt,
                )
                time.sleep(delay)

            except requests.ConnectionError as exc:
                if not self._should_retry_exception(attempt):
                    raise ApiRequestException(
                        str(exc)
                    ) from exc

                if response is not None:
                    response.close()

                delay = self.retry_policy.get_delay(
                    attempt=attempt,
                )
                time.sleep(delay)

            except requests.HTTPError as exc:
                if response is not None:
                    response.close()

                raise ApiRequestException(
                    str(exc)
                ) from exc

            except requests.RequestException as exc:
                if response is not None:
                    response.close()

                raise ApiRequestException(
                    str(exc)
                ) from exc

        raise ApiRequestException(
            f"HTTP request failed after "
            f"{self.retry_policy.max_retries + 1} attempts: "
            f"{method.upper()} {url}"
        )

    def _should_retry_exception(
        self,
        attempt: int,
    ) -> bool:
        """
        Returns True when another attempt is available.
        """

        return attempt < self.retry_policy.max_retries