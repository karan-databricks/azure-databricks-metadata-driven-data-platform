from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from enterprise_api_ingestion_platform.common.exceptions import (
    DuplicateMetadataException,
    MetadataNotFoundException,
)
from enterprise_api_ingestion_platform.models.cdc_metadata import (
    CdcMetadata,
)


class CdcMetadataReader:
    """
    Reads enabled CDC configuration from the Delta metadata table.
    """

    TABLE_NAME = "workspace.config.cdc_metadata"

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def get_metadata(
        self,
        source_id: str,
    ) -> CdcMetadata:
        """
        Returns the enabled CDC configuration for a source.

        Raises
        ------
        MetadataNotFoundException
            If no enabled CDC configuration exists.
        DuplicateMetadataException
            If multiple enabled configurations exist.
        """

        rows = (
            self._spark.table(self.TABLE_NAME)
            .filter(col("source_id") == source_id)
            .filter(col("enabled"))
            .collect()
        )

        if not rows:
            raise MetadataNotFoundException(
                f"CDC metadata not found for source: {source_id}"
            )

        if len(rows) > 1:
            raise DuplicateMetadataException(
                f"Duplicate CDC metadata found for source: {source_id}"
            )

        return self._to_cdc_metadata(rows[0])

    @staticmethod
    def _to_cdc_metadata(
        row,
    ) -> CdcMetadata:
        """
        Converts a Spark Row into a CdcMetadata object.
        """

        return CdcMetadata(
            cdc_config_id=row.cdc_config_id,
            source_id=row.source_id,
            cdc_mode=row.cdc_mode,
            operation_column=row.operation_column,
            sequence_column=row.sequence_column,
            sequence_type=row.sequence_type,
            delete_handling=row.delete_handling,
            snapshot_key_column=row.snapshot_key_column,
            snapshot_strategy=row.snapshot_strategy,
            enabled=row.enabled,
            created_by=row.created_by,
            created_ts=row.created_ts,
            updated_ts=row.updated_ts,
        )