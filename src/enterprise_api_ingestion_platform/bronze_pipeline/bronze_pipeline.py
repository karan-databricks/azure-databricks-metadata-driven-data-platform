# src/enterprise_api_ingestion_platform/bronze_pipeline/bronze_pipeline.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    lit,
)
from pyspark.sql.types import ArrayType, StructType


@dataclass(frozen=True, slots=True)
class BronzeRoute:
    """
    Metadata-driven routing configuration for one API.
    """

    api_id: str
    bronze_catalog: str
    bronze_schema: str
    bronze_table: str
    record_path: str | None


def _required_pipeline_configuration(name: str) -> str:
    """
    Returns a required Lakeflow pipeline configuration value.
    """

    try:
        value = spark.conf.get(name)
    except Exception as exc:
        raise ValueError(
            f"Missing required Bronze pipeline configuration: {name}"
        ) from exc

    if not value or not value.strip():
        raise ValueError(
            f"Missing required Bronze pipeline configuration: {name}"
        )

    return value.strip()


def _landing_volume_root() -> str:
    """
    Builds the Unity Catalog Volume root from pipeline configuration.
    """

    catalog = _required_pipeline_configuration(
        "LANDING_CATALOG",
    )
    schema = _required_pipeline_configuration(
        "LANDING_SCHEMA",
    )
    volume = _required_pipeline_configuration(
        "LANDING_VOLUME",
    )

    return str(
        PurePosixPath(
            "/Volumes",
            catalog,
            schema,
            volume,
        )
    )


def _validate_identifier(
    value: str | None,
    field_name: str,
) -> str:
    """
    Validates one Unity Catalog identifier component.
    """

    if value is None:
        raise ValueError(
            f"{field_name} cannot be null."
        )

    normalized = value.strip()

    if not normalized:
        raise ValueError(
            f"{field_name} cannot be empty."
        )

    if any(
        character in normalized
        for character in (".", "/", "\\")
    ):
        raise ValueError(
            f"{field_name} contains an invalid character: "
            f"{value!r}"
        )

    return normalized


def _validate_record_path(
    value: str | None,
) -> str | None:
    """
    Validates a metadata-driven nested record path.
    """

    if value is None:
        return None

    normalized = value.strip()

    if not normalized:
        return None

    parts = normalized.split(".")

    if any(
        not part.strip()
        or part in {".", ".."}
        or "/" in part
        or "\\" in part
        for part in parts
    ):
        raise ValueError(
            f"Invalid record_path: {value!r}"
        )

    return ".".join(
        part.strip()
        for part in parts
    )


def _load_bronze_routes() -> list[BronzeRoute]:
    """
    Loads enabled Bronze routing configuration from API metadata.
    """

    metadata_table = (
        f"{_required_pipeline_configuration('METADATA_CATALOG')}."
        f"{_required_pipeline_configuration('METADATA_SCHEMA')}."
        "api_metadata"
    )

    rows = (
        spark.table(metadata_table)
        .filter(col("enabled"))
        .select(
            "api_id",
            "bronze_catalog",
            "bronze_schema",
            "bronze_table",
            "record_path",
        )
        .collect()
    )

    routes: list[BronzeRoute] = []

    for row in rows:
        routes.append(
            BronzeRoute(
                api_id=_validate_identifier(
                    row["api_id"],
                    "api_id",
                ),
                bronze_catalog=_validate_identifier(
                    row["bronze_catalog"],
                    "bronze_catalog",
                ),
                bronze_schema=_validate_identifier(
                    row["bronze_schema"],
                    "bronze_schema",
                ),
                bronze_table=_validate_identifier(
                    row["bronze_table"],
                    "bronze_table",
                ),
                record_path=_validate_record_path(
                    row["record_path"],
                ),
            )
        )

    return routes


def _flatten_record_path(
    raw_df: DataFrame,
    record_path: str,
    api_id: str,
) -> DataFrame:
    """
    Extracts and flattens the configured array of API records.
    """

    record_column = col(record_path)

    record_schema = (
        raw_df
        .select(record_column.alias("__bronze_record_source"))
        .schema["__bronze_record_source"]
        .dataType
    )

    if not isinstance(record_schema, ArrayType):
        raise ValueError(
            f"record_path '{record_path}' for API '{api_id}' "
            "must resolve to an array."
        )

    if not isinstance(record_schema.elementType, StructType):
        raise ValueError(
            f"record_path '{record_path}' for API '{api_id}' "
            "must resolve to an array of structs."
        )

    record_fields = record_schema.elementType.fieldNames()

    exploded_df = raw_df.withColumn(
        "__bronze_record",
        explode(record_column),
    )

    technical_columns = [
        col("_source_file"),
        col("_api_id"),
        col("ingestion_timestamp"),
    ]

    business_columns = [
        col(f"__bronze_record.{field}").alias(field)
        for field in record_fields
    ]

    return exploded_df.select(
        *business_columns,
        *technical_columns,
    )


def _prepare_bronze_dataframe(
    raw_df: DataFrame,
    route: BronzeRoute,
) -> DataFrame:
    """
    Converts raw API JSON into the Bronze record contract.
    """

    enriched_df = (
        raw_df
        .withColumn(
            "_source_file",
            col("_metadata.file_path"),
        )
        .withColumn(
            "_api_id",
            lit(route.api_id),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
    )

    if route.record_path is None:
        return enriched_df

    return _flatten_record_path(
        raw_df=enriched_df,
        record_path=route.record_path,
        api_id=route.api_id,
    )


def _create_bronze_table(
    route: BronzeRoute,
    landing_root: str,
) -> None:
    """
    Registers one metadata-driven Bronze streaming table.
    """

    target_table = (
        f"{route.bronze_catalog}."
        f"{route.bronze_schema}."
        f"{route.bronze_table}"
    )

    source_path = str(
        PurePosixPath(
            landing_root,
            route.api_id,
        )
    )

    @dp.table(
        name=target_table,
        comment=(
            "Metadata-driven Bronze table for "
            f"API {route.api_id}."
        ),
    )
    def bronze_table(
        source_path: str = source_path,
        route: BronzeRoute = route,
    ) -> DataFrame:
        """
        Reads one API's raw Volume path into its Bronze table.
        """

        raw_df = (
            spark.readStream
            .format("cloudFiles")
            .option(
                "cloudFiles.format",
                "json",
            )
            .option(
                "cloudFiles.schemaEvolutionMode",
                "addNewColumns",
            )
            .option(
                "cloudFiles.inferColumnTypes",
                "true",
            )
            .option(
                "cloudFiles.rescuedDataColumn",
                "_rescued_data",
            )
            .load(source_path)
        )

        return _prepare_bronze_dataframe(
            raw_df=raw_df,
            route=route,
        )


LANDING_ROOT = _landing_volume_root()

BRONZE_ROUTES = _load_bronze_routes()

for bronze_route in BRONZE_ROUTES:
    _create_bronze_table(
        route=bronze_route,
        landing_root=LANDING_ROOT,
    )