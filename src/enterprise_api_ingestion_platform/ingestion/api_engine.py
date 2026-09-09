# enterprise_api_ingestion_platform\src\enterprise_api_ingestion_platform\ingestion\api_engine.py

from typing import Any

from enterprise_api_ingestion_platform.auth.auth_factory import (
    AuthenticationFactory,
)
from enterprise_api_ingestion_platform.common.exceptions import (
    ApiRequestException,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata

from enterprise_api_ingestion_platform.pagination.pagination_factory import (
    PaginationFactory,
)
from enterprise_api_ingestion_platform.utils.http_client import (
    HttpClient,
)


class ApiEngine:
    """
    Generic metadata-driven REST API execution engine.
    """

    def __init__(self) -> None:
        self.http_client = HttpClient()

    def execute(
        self,
        metadata: ApiMetadata,
    ) -> Any:
        """
        Executes an API using metadata configuration.

        Returns
        -------
        Any
            API response as a Python object.
        """

        auth_provider = AuthenticationFactory.create(metadata)

        headers = {}

        if auth_provider is not None:
            headers.update(auth_provider.get_headers())

        if metadata.headers_json:
            headers.update(metadata.headers_json)

        pagination = PaginationFactory.create(metadata)

        params = pagination.get_initial_params(
            metadata.query_parameters
        )

        results: list[Any] = []

        while True:
            try:
                response_json = self.http_client.request(
                    method=metadata.http_method,
                    url=metadata.endpoint,
                    headers=headers,
                    params=params,
                )

            except Exception as ex:
                raise ApiRequestException(
                    f"Failed to execute API '{metadata.api_name}'."
                ) from ex

            if isinstance(response_json, list):
                results.extend(response_json)
            else:
                results.append(response_json)

            if not pagination.has_next_page(response_json):
                break

            params = pagination.get_next_params(
                params,
                response_json,
            )

        return results