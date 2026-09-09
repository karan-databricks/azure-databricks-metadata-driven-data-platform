# src/enterprise_api_ingestion_platform/gold/transformations.py

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
)


def transform_gold_order_kpis(
    fact_order_df: DataFrame,
) -> DataFrame:
    """
    Aggregates Silver order-line facts into one row per order.

    Order-level totals are retained once per order and are not summed
    across individual order lines.
    """

    return (
        fact_order_df
        .groupBy("order_id")
        .agg(
            spark_max("customer_sk").alias("customer_sk"),
            spark_max("customer_id").alias("customer_id"),
            spark_min("order_date").alias("order_date"),
            spark_max("status").alias("status"),
            spark_max("order_total").alias("order_total"),
            countDistinct("line_number").alias("line_count"),
            spark_sum("quantity").alias("units_sold"),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )


def transform_gold_customer_sales(
    gold_order_kpis_df: DataFrame,
    dim_customer_df: DataFrame,
) -> DataFrame:
    """
    Aggregates Gold order KPIs into one row per customer.

    Order totals are summed once per order because the input is already
    at order grain.
    """

    customer_metrics_df = (
        gold_order_kpis_df
        .groupBy(
            "customer_sk",
            "customer_id",
        )
        .agg(
            countDistinct("order_id").alias("order_count"),
            spark_sum("order_total").alias("sales_amount"),
            spark_sum("units_sold").alias("units_sold"),
        )
    )

    customer_attributes_df = dim_customer_df.select(
        "customer_sk",
        "customer_id",
        "customer_name",
    )

    return (
        customer_metrics_df.alias("metrics")
        .join(
            customer_attributes_df.alias("customers"),
            (
                col("metrics.customer_sk")
                == col("customers.customer_sk")
            )
            & (
                col("metrics.customer_id")
                == col("customers.customer_id")
            ),
            "left",
        )
        .select(
            col("metrics.customer_sk").alias("customer_sk"),
            col("metrics.customer_id").alias("customer_id"),
            col("customers.customer_name").alias(
                "customer_name",
            ),
            col("metrics.order_count").alias("order_count"),
            col("metrics.sales_amount").alias(
                "sales_amount",
            ),
            col("metrics.units_sold").alias("units_sold"),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )


def transform_gold_product_sales(
    fact_order_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """
    Aggregates Silver order-line facts into one row per product.

    Product-level revenue is intentionally not calculated because the
    source provides order-level totals but no product-level amount.
    """

    product_metrics_df = (
        fact_order_df
        .groupBy(
            "product_sk",
            "product_id",
        )
        .agg(
            countDistinct("order_id").alias("order_count"),
            spark_sum("quantity").alias("units_sold"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date"),
        )
    )

    product_attributes_df = dim_product_df.select(
        "product_sk",
        "product_id",
        col("title").alias("product_title"),
        "category",
    )

    return (
        product_metrics_df.alias("metrics")
        .join(
            product_attributes_df.alias("products"),
            (
                col("metrics.product_sk")
                == col("products.product_sk")
            )
            & (
                col("metrics.product_id")
                == col("products.product_id")
            ),
            "left",
        )
        .select(
            col("metrics.product_sk").alias("product_sk"),
            col("metrics.product_id").alias("product_id"),
            col("products.product_title").alias(
                "product_title",
            ),
            col("products.category").alias(
                "category",
            ),
            col("metrics.order_count").alias(
                "order_count",
            ),
            col("metrics.units_sold").alias(
                "units_sold",
            ),
            col("metrics.first_order_date").alias(
                "first_order_date",
            ),
            col("metrics.last_order_date").alias(
                "last_order_date",
            ),
        )
        .withColumn(
            "processed_timestamp",
            current_timestamp(),
        )
    )