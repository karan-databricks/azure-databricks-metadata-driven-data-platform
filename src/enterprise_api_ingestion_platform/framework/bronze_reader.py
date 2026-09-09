from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class BronzeReader:
    """Reads raw JSON files from the landing zone using Auto Loader."""

    @staticmethod
    def read(
        spark: SparkSession,
        metadata: ApiMetadata,
        schema: StructType,
    ) -> DataFrame:
        landing_path = (
            f"/Volumes/"
            f"{metadata.bronze_catalog}/"
            f"bronze/"
            f"landing_volume/"
            f"{metadata.landing_path}"
        )

        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .schema(schema)
            .load(landing_path)
        )