# src/enterprise_api_ingestion_platform/storage/storage_writer_factory.py

from __future__ import annotations

from pyspark.sql import SparkSession

from enterprise_api_ingestion_platform.storage.databricks_storage_writer import (
    DatabricksManagedStorageWriter,
)
from enterprise_api_ingestion_platform.storage.storage_writer import StorageWriter


class StorageWriterFactory:
    """
    Factory for creating StorageWriter implementations.
    """

    @staticmethod
    def create(
        spark: SparkSession,
        landing_catalog: str,
        landing_schema: str,
        landing_volume: str,
    ) -> StorageWriter:
        """
        Creates the Databricks managed-volume writer.
        """

        return DatabricksManagedStorageWriter(
            spark=spark,
            landing_catalog=landing_catalog,
            landing_schema=landing_schema,
            landing_volume=landing_volume,
        )