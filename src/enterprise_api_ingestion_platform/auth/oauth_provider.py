from typing import Dict

import requests

from enterprise_api_ingestion_platform.auth.auth_provider import AuthProvider
from enterprise_api_ingestion_platform.common.exceptions import (
    AuthenticationException,
)


class OAuthProvider(AuthProvider):
    """
    Authentication provider for OAuth2 Client Credentials flow.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope

    def _get_access_token(self) -> str:
        """
        Requests an OAuth2 access token.
        """

        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        if self._scope:
            payload["scope"] = self._scope

        response = requests.post(
            self._token_url,
            data=payload,
            timeout=30,
        )

        if response.status_code != 200:
            raise AuthenticationException(
                "OAuth authentication failed. "
                f"Status Code: {response.status_code}, "
                f"Response: {response.text}"
            )

        token = response.json().get("access_token")

        if not token:
            raise AuthenticationException(
                "OAuth response does not contain an access_token."
            )

        return token

    def get_headers(self) -> Dict[str, str]:
        """
        Returns the Authorization header.
        """

        return {
            "Authorization": f"Bearer {self._get_access_token()}"
        }