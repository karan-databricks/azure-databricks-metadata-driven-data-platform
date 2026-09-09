from __future__ import annotations

from typing import Dict

import requests

from enterprise_api_ingestion_platform.auth.auth_provider import AuthProvider
from enterprise_api_ingestion_platform.common.secret_provider import SecretProvider


class OAuthClientCredentialsProvider(AuthProvider):
    """
    OAuth2 Client Credentials authentication provider.

    Obtains an access token from Microsoft Entra ID and returns
    the Authorization header required for downstream REST APIs.
    """

    TOKEN_SCOPE = "https://graph.microsoft.com/.default"

    def __init__(
        self,
        secret_provider: SecretProvider,
        secret_scope: str,
    ) -> None:
        self._secret_provider = secret_provider
        self._secret_scope = secret_scope

    def get_headers(self) -> Dict[str, str]:
        """
        Returns the Authorization header containing a Bearer token.
        """

        tenant_id = self._secret_provider.get(
            self._secret_scope,
            "tenant-id",
        )

        client_id = self._secret_provider.get(
            self._secret_scope,
            "client-id",
        )

        client_secret = self._secret_provider.get(
            self._secret_scope,
            "client-secret",
        )

        token_endpoint = (
            f"https://login.microsoftonline.com/"
            f"{tenant_id}/oauth2/v2.0/token"
        )

        response = requests.post(
            token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": self.TOKEN_SCOPE,
            },
            timeout=60,
        )

        response.raise_for_status()

        access_token = response.json()["access_token"]

        return {
            "Authorization": f"Bearer {access_token}",
        }