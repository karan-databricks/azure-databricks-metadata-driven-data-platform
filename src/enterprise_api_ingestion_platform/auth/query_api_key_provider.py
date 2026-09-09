from typing import Dict

from enterprise_api_ingestion_platform.auth.auth_provider import (
    AuthProvider,
)


class QueryApiKeyProvider(AuthProvider):
    """
    Authentication provider for APIs that require an API key
    as a query parameter.
    """

    def __init__(
        self,
        api_key: str,
        parameter_name: str = "apiKey",
    ) -> None:
        self._api_key = api_key
        self._parameter_name = parameter_name

    def get_headers(self) -> Dict[str, str]:
        """
        Query parameter authentication does not use HTTP headers.
        """

        return {}

    def get_query_parameters(self) -> Dict[str, str]:
        """
        Returns the query parameter containing the API key.
        """

        return {
            self._parameter_name: self._api_key,
        }