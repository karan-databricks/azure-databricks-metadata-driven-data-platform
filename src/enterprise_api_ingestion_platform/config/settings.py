from dataclasses import dataclass

from enterprise_api_ingestion_platform.common.secret_provider import (
    SecretProvider,
)


@dataclass(frozen=True, slots=True)
class Settings:
    """
    Application configuration.
    """

    landing_container: str = "landing"
    bronze_container: str = "bronze"
    silver_container: str = "silver"
    gold_container: str = "gold"
    metadata_container: str = "metadata"

    secret_scope: str = "api-ingestion-scope"

    @property
    def storage_account(self) -> str:
        """
        Returns the ADLS storage account name.
        """

        return SecretProvider.get(
            scope=self.secret_scope,
            key="storage-account-name",
        )

    @property
    def tenant_id(self) -> str:
        """
        Returns the Microsoft Entra tenant ID.
        """

        return SecretProvider.get(
            scope=self.secret_scope,
            key="tenant-id",
        )

    @property
    def client_id(self) -> str:
        """
        Returns the Service Principal client ID.
        """

        return SecretProvider.get(
            scope=self.secret_scope,
            key="client-id",
        )

    @property
    def client_secret(self) -> str:
        """
        Returns the Service Principal client secret.
        """

        return SecretProvider.get(
            scope=self.secret_scope,
            key="client-secret",
        )


settings = Settings()