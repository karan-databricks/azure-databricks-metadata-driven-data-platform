# src/enterprise_api_ingestion_platform/auth/auth_factory.py

from enterprise_api_ingestion_platform.auth.api_key_provider import (
    ApiKeyProvider,
)
from enterprise_api_ingestion_platform.auth.auth_provider import (
    AuthProvider,
)
from enterprise_api_ingestion_platform.auth.bearer_token_provider import (
    BearerTokenProvider,
)
from enterprise_api_ingestion_platform.auth.oauth_client_credentials_provider import (
    OAuthClientCredentialsProvider,
)
from enterprise_api_ingestion_platform.auth.query_api_key_provider import (
    QueryApiKeyProvider,
)
from enterprise_api_ingestion_platform.auth.salesforce_oauth_client_credentials_provider import (
    SalesforceOAuthClientCredentialsProvider,
)
from enterprise_api_ingestion_platform.common.secret_provider import (
    SecretProvider,
)
from enterprise_api_ingestion_platform.models.api_metadata import (
    ApiMetadata,
)


class AuthenticationFactory:
    """
    Factory responsible for creating authentication providers.
    """

    @staticmethod
    def create(
        metadata: ApiMetadata,
    ) -> AuthProvider | None:
        """
        Creates the authentication provider configured for the API.
        """

        auth_type = (
            metadata.auth_type or "NONE"
        ).strip().upper()

        if auth_type == "NONE":
            return None

        if auth_type == "OAUTH2_CLIENT_CREDENTIALS":
            if not metadata.secret_scope:
                raise ValueError(
                    "secret_scope is required for OAuth2 Client Credentials."
                )

            return OAuthClientCredentialsProvider(
                secret_provider=SecretProvider,
                secret_scope=metadata.secret_scope,
            )

        if auth_type == "SALESFORCE_OAUTH2_CLIENT_CREDENTIALS":
            if not metadata.secret_scope:
                raise ValueError(
                    "secret_scope is required for Salesforce OAuth2."
                )

            if not metadata.endpoint:
                raise ValueError(
                    "endpoint is required for Salesforce OAuth2."
                )

            return SalesforceOAuthClientCredentialsProvider(
                secret_provider=SecretProvider,
                secret_scope=metadata.secret_scope,
                api_endpoint=metadata.endpoint,
            )

        if (
            not metadata.secret_scope
            or not metadata.secret_key
        ):
            raise ValueError(
                "secret_scope and secret_key are required "
                "for authenticated APIs."
            )

        secret_value = SecretProvider.get(
            scope=metadata.secret_scope,
            key=metadata.secret_key,
        )

        if auth_type == "API_KEY_HEADER":
            return ApiKeyProvider(
                api_key=secret_value,
                header_name=(
                    metadata.auth_header_name
                    or "X-Api-Key"
                ),
                header_prefix=metadata.auth_header_prefix,
            )

        if auth_type == "API_KEY_QUERY":
            return QueryApiKeyProvider(
                api_key=secret_value,
                parameter_name=(
                    metadata.auth_header_name
                    or "apiKey"
                ),
            )

        if auth_type == "BEARER_TOKEN":
            return BearerTokenProvider(
                token=secret_value,
                header_name=(
                    metadata.auth_header_name
                    or "Authorization"
                ),
            )

        if auth_type == "BASIC_AUTH":
            raise NotImplementedError(
                "Basic Authentication is not implemented yet."
            )

        raise ValueError(
            f"Unsupported authentication type: {auth_type}"
        )