from enterprise_api_ingestion_platform.auth.auth_provider import (
    AuthProvider,
)


class BearerTokenProvider(AuthProvider):
    """
    Authentication provider for Bearer Token authentication.
    """

    def __init__(
        self,
        token: str,
        header_name: str = "Authorization",
    ) -> None:
        self._token = token
        self._header_name = header_name

    def get_headers(self) -> dict[str, str]:
        """
        Returns the HTTP Authorization header.
        """

        return {
            self._header_name: f"Bearer {self._token}",
        }

    def get_query_parameters(self) -> dict[str, str]:
        """
        Bearer authentication does not require query parameters.
        """

        return {}