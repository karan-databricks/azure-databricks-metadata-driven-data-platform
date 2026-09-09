# src/enterprise_api_ingestion_platform/auth/salesforce_oauth_client_credentials_provider.py

from __future__ import annotations

from typing import Dict
from urllib.parse import urlsplit

import requests

from enterprise_api_ingestion_platform.auth.auth_provider import AuthProvider
from enterprise_api_ingestion_platform.common.secret_provider import SecretProvider


class SalesforceOAuthClientCredentialsProvider(AuthProvider):
    """
    Obtains a Salesforce OAuth access token using client credentials.

    The Salesforce instance URL is derived from the configured API endpoint.
    """

    TOKEN_PATH = "/services/oauth2/token"
    CLIENT_ID_KEY = "salesforce-client-id"
    CLIENT_SECRET_KEY = "salesforce-client-secret"
    DEFAULT_TIMEOUT_SECONDS = 60

    def __init__(
        self,
        secret_provider: SecretProvider,
        secret_scope: str,
        api_endpoint: str,
    ) -> None:
        self._secret_provider = secret_provider
        self._secret_scope = secret_scope
        self._instance_url = self._get_instance_url(api_endpoint)

    @staticmethod
    def _get_instance_url(
        api_endpoint: str,
    ) -> str:
        """
        Extracts the Salesforce instance URL from the API endpoint.
        """

        parsed = urlsplit(api_endpoint)

        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Invalid Salesforce API endpoint: {api_endpoint}"
            )

        return f"{parsed.scheme}://{parsed.netloc}"

    def get_headers(self) -> Dict[str, str]:
        """
        Returns the Authorization header required by Salesforce.
        """

        client_id = self._secret_provider.get(
            self._secret_scope,
            self.CLIENT_ID_KEY,
        )

        client_secret = self._secret_provider.get(
            self._secret_scope,
            self.CLIENT_SECRET_KEY,
        )

        token_endpoint = (
            f"{self._instance_url}{self.TOKEN_PATH}"
        )

        response = requests.post(
            token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=self.DEFAULT_TIMEOUT_SECONDS,
        )

        response.raise_for_status()

        response_data = response.json()

        access_token = response_data.get("access_token")

        if not access_token:
            raise ValueError(
                "Salesforce OAuth response did not contain "
                "an access_token."
            )

        return {
            "Authorization": f"Bearer {access_token}",
        }