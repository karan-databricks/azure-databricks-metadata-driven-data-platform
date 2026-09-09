# src/enterprise_api_ingestion_platform/silver/silver_pipeline.py

from __future__ import annotations

from dataclasses import dataclass

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    row_number,
    to_date,
    to_json,
)
from pyspark.sql.types import DecimalType

from transformations import (
    transform_dim_customer,
    transform_dim_product,
    transform_fact_order,
)


METADATA_TABLE = "workspace.config.api_metadata"

SALESFORCE_API_ID = "salesforce_opportunity"
OPENAPI_USERS_API_ID = "openapi_users"
OPENAPI_PRODUCTS_API_ID = "openapi_products"
OPENAPI_ORDERS_API_ID = "openapi_orders"

LATEST_TABLE = "salesforce_opportunity_latest"
SCD2_TABLE = "salesforce_opportunity_scd2"
SALESFORCE_SILVER_TABLE = "salesforce_opportunity"

CUSTOMER_TABLE = "dim_customer"
PRODUCT_TABLE = "dim_product"
ORDER_TABLE = "fact_order"


@dataclass(frozen=True, slots=True)
class SilverRoute:
    """
    Metadata-driven routing configuration for one Silver API model.
    """

    api_id: str
    bronze_catalog: str
    bronze_schema: str
    bronze_table: str

    @property
    def bronze_fqn(self) -> str:
        """
        Returns the fully qualified Bronze table name.
        """

        return (
            f"{self.bronze_catalog}."
            f"{self.bronze_schema}."
            f"{self.bronze_table}"
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


def _get_spark() -> SparkSession:
    """
    Returns the active Spark session.
    """

    spark_session = SparkSession.getActiveSession()

    if spark_session is None:
        raise RuntimeError(
            "No active Spark session is available."
        )

    return spark_session


def _load_silver_routes() -> list[SilverRoute]:
    """
    Loads all enabled Silver source routes from API metadata.
    """

    spark_session = _get_spark()

    rows = (
        spark_session.table(METADATA_TABLE)
        .filter(col("enabled"))
        .select(
            "api_id",
            "bronze_catalog",
            "bronze_schema",
            "bronze_table",
        )
        .orderBy(col("api_id"))
        .collect()
    )

    routes: list[SilverRoute] = []

    for row in rows:
        routes.append(
            SilverRoute(
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
            )
        )

    return routes


def _get_route(
    routes: list[SilverRoute],
    api_id: str,
) -> SilverRoute:
    """
    Returns the unique enabled Silver route for an API.
    """

    matching_routes = [
        route
        for route in routes
        if route.api_id == api_id
    ]

    if not matching_routes:
        raise ValueError(
            f"No enabled API metadata found for api_id={api_id!r}."
        )

    if len(matching_routes) > 1:
        raise ValueError(
            f"Multiple enabled API metadata rows found "
            f"for api_id={api_id!r}."
        )

    return matching_routes[0]


SILVER_ROUTES = _load_silver_routes()

SALESFORCE_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=SALESFORCE_API_ID,
)

OPENAPI_USERS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_USERS_API_ID,
)

OPENAPI_PRODUCTS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_PRODUCTS_API_ID,
)

OPENAPI_ORDERS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_ORDERS_API_ID,
)


@dp.materialized_view(
    name=LATEST_TABLE,
    comment=(
        "Latest Salesforce Opportunity snapshot prepared for "
        "SCD Type 2 processing."
    ),
)
def salesforce_opportunity_latest() -> DataFrame:
    """
    Produces one deterministic source record per Salesforce Opportunity.

    The newest Bronze ingestion record wins when multiple records
    exist for the same Salesforce business key.
    """

    spark_session = _get_spark()

    bronze_df = spark_session.read.table(
        SALESFORCE_ROUTE.bronze_fqn,
    )

    window = Window.partitionBy(
        "Id",
    ).orderBy(
        col("ingestion_timestamp").desc(),
        col("Id").desc(),
    )

    return (
        bronze_df
        .withColumn(
            "_row_number",
            row_number().over(window),
        )
        .filter(
            col("_row_number") == 1,
        )
        .drop(
            "_row_number",
        )
        .select(
            col("Id").alias("opportunity_id"),
            col("Name").alias("name"),
            col("Amount")
            .cast(
                DecimalType(18, 2),
            )
            .alias("amount"),
            col("StageName").alias("stage_name"),
            to_date(
                col("CloseDate"),
            ).alias("close_date"),
            to_json(
                col("attributes"),
            ).alias("source_attributes"),
            col("ingestion_timestamp"),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )


dp.create_streaming_table(
    name=SCD2_TABLE,
    comment=(
        "Internal SCD Type 2 history for Salesforce Opportunities."
    ),
    schema="""
        opportunity_id STRING,
        name STRING,
        amount DECIMAL(18,2),
        stage_name STRING,
        close_date DATE,
        source_attributes STRING,
        ingestion_timestamp TIMESTAMP,
        processed_timestamp TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
)


dp.create_auto_cdc_from_snapshot_flow(
    target=SCD2_TABLE,
    source=LATEST_TABLE,
    keys=[
        "opportunity_id",
    ],
    stored_as_scd_type="2",
    track_history_column_list=[
        "name",
        "amount",
        "stage_name",
        "close_date",
        "source_attributes",
    ],
)


@dp.materialized_view(
    name=SALESFORCE_SILVER_TABLE,
    comment=(
        "Silver SCD Type 2 table for Salesforce Opportunities."
    ),
)
def salesforce_opportunity_silver() -> DataFrame:
    """
    Presents the internal SCD Type 2 history as the public Silver model.

    The surrogate key is generated deterministically from the complete
    historical record ordering.
    """

    spark_session = _get_spark()

    scd2_df = spark_session.read.table(
        SCD2_TABLE,
    )

    surrogate_key_window = Window.orderBy(
        col("opportunity_id").asc(),
        col("__START_AT").asc(),
        col("__END_AT").asc_nulls_last(),
    )

    return (
        scd2_df
        .withColumn(
            "opportunity_sk",
            row_number().over(
                surrogate_key_window,
            ).cast("long"),
        )
        .select(
            "opportunity_sk",
            "opportunity_id",
            "name",
            "amount",
            "stage_name",
            "close_date",
            "source_attributes",
            "ingestion_timestamp",
            "processed_timestamp",
            col("__START_AT").alias(
                "effective_start_ts",
            ),
            col("__END_AT").alias(
                "effective_end_ts",
            ),
            (
                col("__END_AT").isNull()
            ).alias(
                "is_active",
            ),
        )
    )


@dp.materialized_view(
    name=CUSTOMER_TABLE,
    comment=(
        "Silver customer dimension derived from OpenAPI Users."
    ),
)
def dim_customer() -> DataFrame:
    """
    Produces the SCD Type 1 customer dimension.
    """

    spark_session = _get_spark()

    bronze_df = spark_session.read.table(
        OPENAPI_USERS_ROUTE.bronze_fqn,
    )

    return transform_dim_customer(
        bronze_df,
    )


@dp.materialized_view(
    name=PRODUCT_TABLE,
    comment=(
        "Silver product dimension derived from OpenAPI Products."
    ),
)
def dim_product() -> DataFrame:
    """
    Produces the SCD Type 1 product dimension.
    """

    spark_session = _get_spark()

    bronze_df = spark_session.read.table(
        OPENAPI_PRODUCTS_ROUTE.bronze_fqn,
    )

    return transform_dim_product(
        bronze_df,
    )


def _read_fact_order_source() -> DataFrame:
    """
    Reads the existing Bronze order snapshot source.
    """

    spark_session = _get_spark()

    return spark_session.read.table(
        OPENAPI_ORDERS_ROUTE.bronze_fqn,
    )


@dp.materialized_view(
    name=ORDER_TABLE,
    comment=(
        "Silver order-line fact derived from OpenAPI Orders."
    ),
)
def fact_order() -> DataFrame:
    """
    Produces one Silver fact row per product line within an order.
    """

    bronze_df = _read_fact_order_source()

    spark_session = _get_spark()

    customer_df = spark_session.read.table(
        CUSTOMER_TABLE,
    )

    product_df = spark_session.read.table(
        PRODUCT_TABLE,
    )

    return transform_fact_order(
        bronze_df=bronze_df,
        customer_df=customer_df,
        product_df=product_df,
    )