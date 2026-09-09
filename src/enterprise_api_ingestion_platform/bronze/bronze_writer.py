import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
)

from enterprise_api_ingestion_platform.common.exceptions import (
    BronzeWriteException,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class BronzeWriter:
    """
    Writes raw API responses to the Bronze layer.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(
        self,
        metadata: ApiMetadata,
        api_response: list[dict],
    ) -> None:
        """
        Writes raw API response to the configured Bronze table.
        """

        try:

            rows = [
                (
                    datetime.utcnow(),
                    metadata.api_name,
                    json.dumps(record),
                )
                for record in api_response
            ]

            schema = StructType(
                [
                    StructField(
                        "ingest_timestamp",
                        TimestampType(),
                        False,
                    ),
                    StructField(
                        "api_name",
                        StringType(),
                        False,
                    ),
                    StructField(
                        "raw_payload",
                        StringType(),
                        False,
                    ),
                ]
            )

            dataframe = self.spark.createDataFrame(
                rows,
                schema=schema,
            )

            table_name = (
                f"{metadata.bronze_catalog}."
                f"{metadata.bronze_schema}."
                f"{metadata.bronze_table}"
            )

            (
                dataframe.write
                .format("delta")
                .mode("append")
                .saveAsTable(table_name)
            )

        except Exception as exc:
            raise BronzeWriteException(
                f"Failed to write Bronze table: {exc}"
            ) from exc