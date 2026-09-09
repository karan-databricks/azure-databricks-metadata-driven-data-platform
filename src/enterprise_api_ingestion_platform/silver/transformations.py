# src/enterprise_api_ingestion_platform/silver/transformations.py

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    posexplode,
    row_number,
    sha2,
    to_timestamp,
)


def _latest_records(
    bronze_df: DataFrame,
    business_key_column: str,
) -> DataFrame:
    """
    Retains the newest Bronze record for each business key.

    Ingestion timestamp is the primary ordering criterion and source
    file provides deterministic tie-breaking.
    """

    latest_window = Window.partitionBy(
        business_key_column,
    ).orderBy(
        col("ingestion_timestamp").desc(),
        col("_source_file").desc(),
    )

    return (
        bronze_df
        .withColumn(
            "_row_number",
            row_number().over(latest_window),
        )
        .filter(
            col("_row_number") == 1,
        )
        .drop(
            "_row_number",
        )
    )


def transform_dim_customer(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transforms OpenAPI Users Bronze records into the customer dimension.

    One record is retained per customer ID, with the newest ingestion
    record selected deterministically.
    """

    latest_df = _latest_records(
        bronze_df=bronze_df,
        business_key_column="id",
    )

    return (
        latest_df
        .select(
            sha2(
                col("id").cast("string"),
                256,
            ).alias("customer_sk"),
            col("id").alias("customer_id"),
            col("name").alias("customer_name"),
            col("username"),
            col("email"),
            col("avatar"),
            col("role"),
            col("phone"),
            col("website"),
            col("address.street").alias("address_street"),
            col("address.suite").alias("address_suite"),
            col("address.city").alias("address_city"),
            col("address.zipcode").alias("address_zipcode"),
            col("address.geo.lat").alias("address_lat"),
            col("address.geo.lng").alias("address_lng"),
            col("company.name").alias("company_name"),
            col("company.catchPhrase").alias(
                "company_catch_phrase",
            ),
            col("company.bs").alias("company_bs"),
            col("ingestion_timestamp"),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )


def transform_dim_product(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transforms OpenAPI Products Bronze records into the product dimension.

    One record is retained per product ID, with the newest ingestion
    record selected deterministically.
    """

    latest_df = _latest_records(
        bronze_df=bronze_df,
        business_key_column="id",
    )

    return (
        latest_df
        .select(
            sha2(
                col("id").cast("string"),
                256,
            ).alias("product_sk"),
            col("id").alias("product_id"),
            col("title").alias("title"),
            col("description"),
            col("category"),
            col("price"),
            col("image"),
            col("rating.rate").alias("rating_rate"),
            col("rating.count").alias("rating_count"),
            col("ingestion_timestamp"),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )


def transform_fact_order(
    bronze_df: DataFrame,
    customer_df: DataFrame,
    product_df: DataFrame,
) -> DataFrame:
    """
    Transforms OpenAPI Orders Bronze records into an order-line fact.

    The grain is one product line within one order. The combination of
    order_id and line_number uniquely identifies a fact row.
    """

    latest_orders_df = _latest_records(
        bronze_df=bronze_df,
        business_key_column="id",
    )

    order_lines_df = (
        latest_orders_df
        .select(
            col("id").alias("order_id"),
            col("userId").alias("customer_id"),
            col("date").alias("order_date"),
            col("status"),
            col("total").alias("order_total"),
            col("ingestion_timestamp"),
            posexplode(
                col("products"),
            ).alias(
                "line_position",
                "product",
            ),
        )
        .select(
            "order_id",
            "customer_id",
            (
                col("line_position") + 1
            ).cast("int").alias(
                "line_number",
            ),
            col("product.productId").alias(
                "product_id",
            ),
            col("product.quantity").alias(
                "quantity",
            ),
            to_timestamp(
                col("order_date"),
            ).alias(
                "order_date",
            ),
            "status",
            "order_total",
            "ingestion_timestamp",
        )
    )

    customer_lookup_df = customer_df.select(
        col("customer_id").alias("_customer_id"),
        col("customer_sk"),
    )

    product_lookup_df = product_df.select(
        col("product_id").alias("_product_id"),
        col("product_sk"),
    )

    enriched_df = (
        order_lines_df
        .join(
            customer_lookup_df,
            col("customer_id") == col("_customer_id"),
            "left",
        )
        .drop(
            "_customer_id",
        )
        .join(
            product_lookup_df,
            col("product_id") == col("_product_id"),
            "left",
        )
        .drop(
            "_product_id",
        )
    )

    return (
        enriched_df
        .select(
            sha2(
                concat_ws(
                    "|",
                    col("order_id").cast("string"),
                    col("line_number").cast("string"),
                ),
                256,
            ).alias("order_sk"),
            "order_id",
            "line_number",
            "customer_sk",
            "customer_id",
            "product_sk",
            "product_id",
            "quantity",
            "order_date",
            "status",
            "order_total",
            "ingestion_timestamp",
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )