from pyspark.sql import SparkSession

from enterprise_api_ingestion_platform.common.secret_provider import (
    SecretProvider,
)
from enterprise_api_ingestion_platform.config.settings import settings


class StorageConfiguration:
    """
    Configures Spark to authenticate with Azure Data Lake Storage Gen2.
    """

    @staticmethod
    def configure(spark: SparkSession) -> None:
        """
        Configure Spark for ADLS Gen2 access using secrets stored in the
        Databricks Secret Scope.
        """

        storage_account = settings.storage_account

        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
            "OAuth",
        )

        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )

        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
            SecretProvider.get("client-id"),
        )

        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
            SecretProvider.get("client-secret"),
        )

        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
            SecretProvider.get("tenant-endpoint"),
        )