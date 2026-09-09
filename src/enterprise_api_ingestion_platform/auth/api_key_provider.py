from typing import Dict

from enterprise_api_ingestion_platform.auth.auth_provider import (
    AuthProvider,
)


class ApiKeyProvider(AuthProvider):
    """
    Authentication provider for APIs that require an API key.
    """

    def __init__(
        self,
        api_key: str,
        header_name: str = "x-api-key",
        header_prefix: str | None = None,
    ) -> None:
        self._api_key = api_key
        self._header_name = header_name
        self._header_prefix = header_prefix

    def get_headers(self) -> Dict[str, str]:
        """
        Returns the HTTP headers containing the API key.
        """

        value = self._api_key

        if self._header_prefix:
            value = (
                f"{self._header_prefix} "
                f"{self._api_key}"
            )

        return {
            self._header_name: value,
        }