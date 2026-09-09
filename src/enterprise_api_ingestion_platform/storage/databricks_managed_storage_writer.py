from __future__ import annotations

import json
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from enterprise_api_ingestion_platform.logging.logger import get_logger
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.storage.storage_writer import StorageWriter

logger = get_logger(__name__)


class DatabricksManagedStorageWriter(StorageWriter):
    """
    Writes API ingestion payloads to Unity Catalog managed Delta tables.

    The target table is resolved entirely from API metadata, so this
    implementation does not require direct ADLS Gen2 credentials.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def write(
        self,
        metadata: ApiMetadata,
        payload: Any,
    ) -> str:
        """
        Writes a buffered API payload to the configured Bronze table.
        """

        target_table = self._target_table(metadata)

        records = self._extract_records(payload)

        if not records:
            logger.warning(
                "API returned no records. api=%s target=%s",
                metadata.api_name,
                target_table,
            )

            self._write_empty_result(
                metadata=metadata,
                target_table=target_table,
            )

            return target_table

        dataframe = self._create_dataframe(
            records=records,
        )

        dataframe = dataframe.withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )

        mode = self._write_mode(metadata)

        logger.info(
            "Writing API records. api=%s target=%s mode=%s records=%s",
            metadata.api_name,
            target_table,
            mode,
            len(records),
        )

        (
            dataframe.write
            .format("delta")
            .mode(mode)
            .saveAsTable(target_table)
        )

        logger.info(
            "Managed table write completed. target=%s",
            target_table,
        )

        return target_table

    def write_stream(
        self,
        metadata: ApiMetadata,
        chunks: Iterable[bytes],
    ) -> str:
        """
        Writes streamed JSON content to the configured Bronze table.

        The current implementation buffers the chunks so that the same
        record extraction and table-writing logic is used as BUFFER mode.
        """

        payload = b"".join(
            chunk
            for chunk in chunks
            if chunk
        )

        if not payload:
            return self.write(
                metadata=metadata,
                payload={},
            )

        try:
            decoded_payload = json.loads(
                payload.decode("utf-8"),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "Streaming response is not valid UTF-8 JSON."
            ) from exc

        return self.write(
            metadata=metadata,
            payload=decoded_payload,
        )

    @staticmethod
    def _target_table(
        metadata: ApiMetadata,
    ) -> str:
        """
        Builds and validates the fully-qualified managed table name.
        """

        parts = (
            metadata.bronze_catalog,
            metadata.bronze_schema,
            metadata.bronze_table,
        )

        if not all(parts):
            raise ValueError(
                "bronze_catalog, bronze_schema, and bronze_table "
                "are required for Databricks managed storage."
            )

        return ".".join(parts)

    @staticmethod
    def _extract_records(
        payload: Any,
    ) -> list[dict[str, Any]]:
        """
        Extracts tabular records from common API response structures.
        """

        if isinstance(payload, dict):
            records = payload.get("records")

            if records is not None:
                if not isinstance(records, list):
                    raise ValueError(
                        "API response field 'records' must be a list."
                    )

                return [
                    record
                    for record in records
                    if isinstance(record, dict)
                ]

            value = payload.get("value")

            if value is not None:
                if not isinstance(value, list):
                    raise ValueError(
                        "API response field 'value' must be a list."
                    )

                return [
                    record
                    for record in value
                    if isinstance(record, dict)
                ]

            return [payload]

        if isinstance(payload, list):
            return [
                record
                for record in payload
                if isinstance(record, dict)
            ]

        raise ValueError(
            "API response must be a dictionary or list of dictionaries."
        )

    def _create_dataframe(
        self,
        records: list[dict[str, Any]],
    ) -> DataFrame:
        """
        Creates a Spark DataFrame from API records.
        """

        dataframe = self._spark.createDataFrame(
            records,
        )

        return dataframe

    @staticmethod
    def _write_mode(
        metadata: ApiMetadata,
    ) -> str:
        """
        Resolves the table write mode from load metadata.
        """

        load_type = (
            metadata.load_type or "FULL"
        ).strip().upper()

        if load_type == "FULL":
            return "overwrite"

        return "append"

    def _write_empty_result(
        self,
        metadata: ApiMetadata,
        target_table: str,
    ) -> None:
        """
        Creates the managed table when a full load returns no records.
        """

        schema = self._spark.createDataFrame(
            [],
            "ingestion_timestamp timestamp",
        ).schema

        empty_dataframe = self._spark.createDataFrame(
            [],
            schema,
        )

        mode = self._write_mode(metadata)

        (
            empty_dataframe.write
            .format("delta")
            .mode(mode)
            .saveAsTable(target_table)
        )