# src/enterprise_api_ingestion_platform/landing/landing_service.py

from __future__ import annotations

from typing import Any

from enterprise_api_ingestion_platform.auth.auth_factory import (
    AuthenticationFactory,
)
from enterprise_api_ingestion_platform.landing.landing_writer import (
    LandingWriter,
)
from enterprise_api_ingestion_platform.logging.logger import (
    get_logger,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.models.landing_result import (
    LandingResult,
)
from enterprise_api_ingestion_platform.pagination.pagination_factory import (
    PaginationFactory,
)
from enterprise_api_ingestion_platform.utils.http_client import HttpClient


logger = get_logger(__name__)


class LandingService:
    """
    Orchestrates retrieval of API data and persists the payload
    into the Landing zone.
    """

    def __init__(
        self,
        http_client: HttpClient,
        landing_writer: LandingWriter,
    ) -> None:
        self._http_client = http_client
        self._landing_writer = landing_writer

    def ingest(
        self,
        metadata: ApiMetadata,
        initial_cursor: str | None = None,
    ) -> LandingResult:
        """
        Executes a single landing ingestion.

        Parameters
        ----------
        metadata
            API metadata configuration used to execute the request and
            write the landing data.

        initial_cursor
            Previously persisted cursor used to start cursor pagination.

        Returns
        -------
        LandingResult
            Result of the landing write including ingestion metrics.
        """

        auth_provider = AuthenticationFactory.create(
            metadata,
        )

        headers: dict[str, str] = {}
        query_parameters: dict[str, Any] = {}

        if metadata.query_parameters:
            query_parameters.update(
                metadata.query_parameters,
            )

        if metadata.headers_json:
            headers.update(
                metadata.headers_json,
            )

        if auth_provider is not None:
            headers.update(
                auth_provider.get_headers(),
            )

            query_parameters.update(
                auth_provider.get_query_parameters(),
            )

        use_streaming = (
            (metadata.download_mode or "BUFFER").upper()
            == "STREAM"
        )

        logger.info(
            "API=%s download_mode=%s use_streaming=%s",
            metadata.api_name,
            metadata.download_mode,
            use_streaming,
        )

        if use_streaming:
            chunks = self._http_client.stream(
                method=metadata.http_method,
                url=metadata.endpoint,
                headers=headers,
                params=query_parameters,
            )

            landing_file = self._landing_writer.write_stream(
                metadata=metadata,
                chunks=chunks,
            )

            landing_file_size_bytes = (
                self._landing_writer.get_file_size(
                    file_path=landing_file,
                )
            )

            return LandingResult(
                landing_file=landing_file,
                records_read=None,
                pages_read=None,
                landing_file_size_bytes=landing_file_size_bytes,
            )

        pagination = PaginationFactory.create(
            metadata,
        )

        params = pagination.get_initial_params(
            query_parameters,
        )

        if (
            initial_cursor is not None
            and (metadata.pagination_type or "NONE").upper()
            == "CURSOR"
        ):
            params[metadata.cursor_parameter] = initial_cursor

        response_json = self._http_client.request(
            method=metadata.http_method,
            url=metadata.endpoint,
            headers=headers,
            params=params,
        )

        first_page_record_count = self._count_records(
            response_json,
        )

        if not pagination.has_next_page(
            response_json,
        ):
            landing_file = self._landing_writer.write(
                metadata=metadata,
                payload=response_json,
            )

            landing_file_size_bytes = (
                self._landing_writer.get_file_size(
                    file_path=landing_file,
                )
            )

            return LandingResult(
                landing_file=landing_file,
                checkpoint_state=self._get_cursor(
                    metadata=metadata,
                    response_json=response_json,
                ),
                records_read=first_page_record_count,
                pages_read=1,
                landing_file_size_bytes=landing_file_size_bytes,
            )

        pages: list[Any] = [response_json]

        records_read = first_page_record_count

        checkpoint_state = self._get_cursor(
            metadata=metadata,
            response_json=response_json,
        )

        while pagination.has_next_page(
            response_json,
        ):
            params = pagination.get_next_params(
                params,
                response_json,
            )

            response_json = self._http_client.request(
                method=metadata.http_method,
                url=metadata.endpoint,
                headers=headers,
                params=params,
            )

            pages.append(response_json)

            records_read += self._count_records(
                response_json,
            )

            cursor = self._get_cursor(
                metadata=metadata,
                response_json=response_json,
            )

            if cursor is not None:
                checkpoint_state = cursor

        landing_file = self._landing_writer.write(
            metadata=metadata,
            payload=pages,
        )

        landing_file_size_bytes = (
            self._landing_writer.get_file_size(
                file_path=landing_file,
            )
        )

        return LandingResult(
            landing_file=landing_file,
            checkpoint_state=checkpoint_state,
            records_read=records_read,
            pages_read=len(pages),
            landing_file_size_bytes=landing_file_size_bytes,
        )

    @staticmethod
    def _count_records(
        response_json: Any,
    ) -> int:
        """
        Returns the number of API records represented by a response.

        A top-level list is treated as the record collection.

        For dictionary responses, the method looks for common
        collection fields. If no collection can be identified,
        the response is treated as one logical API response record.
        """

        if isinstance(response_json, list):
            return len(response_json)

        if isinstance(response_json, dict):
            for field_name in (
                "data",
                "results",
                "records",
                "items",
            ):
                value = response_json.get(field_name)

                if isinstance(value, list):
                    return len(value)

            return 1

        return 1

    @staticmethod
    def _get_cursor(
        metadata: ApiMetadata,
        response_json: Any,
    ) -> str | None:
        """
        Returns the configured cursor from a response.
        """

        if (
            (metadata.pagination_type or "NONE").upper()
            != "CURSOR"
        ):
            return None

        if not isinstance(response_json, dict):
            return None

        cursor = response_json.get(
            metadata.cursor_field,
        )

        if cursor is None or cursor == "":
            return None

        return str(cursor)