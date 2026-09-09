# tests/gold/test_gold_customer_sales.py

from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.gold.transformations import (
    transform_gold_customer_sales,
)


def _gold_order_kpis_schema() -> StructType:
    """
    Returns the production-compatible gold_order_kpis test schema.
    """

    return StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("customer_sk", StringType(), True),
            StructField("customer_id", LongType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("order_total", LongType(), True),
            StructField("line_count", LongType(), True),
            StructField("units_sold", LongType(), True),
            StructField(
                "processed_timestamp",
                TimestampType(),
                True,
            ),
        ]
    )


def _dim_customer_schema() -> StructType:
    """
    Returns the production-compatible dim_customer test schema.
    """

    return StructType(
        [
            StructField("customer_sk", StringType(), False),
            StructField("customer_id", LongType(), False),
            StructField("customer_name", StringType(), True),
        ]
    )


def _order_kpi(
    order_id: int,
    customer_sk: str,
    customer_id: int,
    order_total: int,
    units_sold: int,
) -> tuple:
    """
    Creates one production-compatible Gold order KPI row.
    """

    return (
        order_id,
        customer_sk,
        customer_id,
        datetime(
            2025,
            12,
            19,
            10,
            0,
            0,
        ),
        "delivered",
        order_total,
        2,
        units_sold,
        datetime(
            2026,
            8,
            29,
            10,
            5,
            0,
        ),
    )


def test_gold_customer_sales_aggregates_customer_orders(
    spark,
) -> None:
    """
    Verifies multiple orders belonging to one customer are aggregated.
    """

    order_kpis_df = spark.createDataFrame(
        [
            _order_kpi(
                order_id=1,
                customer_sk="customer-1",
                customer_id=71,
                order_total=1082,
                units_sold=5,
            ),
            _order_kpi(
                order_id=2,
                customer_sk="customer-1",
                customer_id=71,
                order_total=589,
                units_sold=4,
            ),
            _order_kpi(
                order_id=3,
                customer_sk="customer-2",
                customer_id=81,
                order_total=750,
                units_sold=3,
            ),
        ],
        schema=_gold_order_kpis_schema(),
    )

    dim_customer_df = spark.createDataFrame(
        [
            (
                "customer-1",
                71,
                "Customer One",
            ),
            (
                "customer-2",
                81,
                "Customer Two",
            ),
        ],
        schema=_dim_customer_schema(),
    )

    result = transform_gold_customer_sales(
        gold_order_kpis_df=order_kpis_df,
        dim_customer_df=dim_customer_df,
    )

    rows = {
        row.customer_id: row
        for row in result.collect()
    }

    assert len(rows) == 2

    customer_one = rows[71]

    assert customer_one.order_count == 2
    assert customer_one.sales_amount == 1671
    assert customer_one.units_sold == 9


def test_gold_customer_sales_returns_one_row_per_customer(
    spark,
) -> None:
    """
    Verifies the Gold customer mart has customer grain.
    """

    order_kpis_df = spark.createDataFrame(
        [
            _order_kpi(
                order_id=1,
                customer_sk="customer-1",
                customer_id=71,
                order_total=100,
                units_sold=2,
            ),
            _order_kpi(
                order_id=2,
                customer_sk="customer-1",
                customer_id=71,
                order_total=200,
                units_sold=3,
            ),
            _order_kpi(
                order_id=3,
                customer_sk="customer-2",
                customer_id=81,
                order_total=300,
                units_sold=4,
            ),
        ],
        schema=_gold_order_kpis_schema(),
    )

    dim_customer_df = spark.createDataFrame(
        [
            (
                "customer-1",
                71,
                "Customer One",
            ),
            (
                "customer-2",
                81,
                "Customer Two",
            ),
        ],
        schema=_dim_customer_schema(),
    )

    result = transform_gold_customer_sales(
        gold_order_kpis_df=order_kpis_df,
        dim_customer_df=dim_customer_df,
    )

    assert result.count() == 2
    assert result.select("customer_id").distinct().count() == 2


def test_gold_customer_sales_does_not_multiply_order_total(
    spark,
) -> None:
    """
    Verifies order totals are summed once per order.

    The input is already at order grain, so each order_total appears
    exactly once.
    """

    order_kpis_df = spark.createDataFrame(
        [
            _order_kpi(
                order_id=10,
                customer_sk="customer-10",
                customer_id=100,
                order_total=1000,
                units_sold=5,
            ),
            _order_kpi(
                order_id=11,
                customer_sk="customer-10",
                customer_id=100,
                order_total=500,
                units_sold=2,
            ),
        ],
        schema=_gold_order_kpis_schema(),
    )

    dim_customer_df = spark.createDataFrame(
        [
            (
                "customer-10",
                100,
                "Customer Ten",
            ),
        ],
        schema=_dim_customer_schema(),
    )

    result = transform_gold_customer_sales(
        gold_order_kpis_df=order_kpis_df,
        dim_customer_df=dim_customer_df,
    )

    row = result.first()

    assert row.sales_amount == 1500
    assert row.sales_amount != 3000
    assert row.units_sold == 7


def test_gold_customer_sales_preserves_customer_identity(
    spark,
) -> None:
    """
    Verifies customer surrogate key, business ID, and name are preserved.
    """

    order_kpis_df = spark.createDataFrame(
        [
            _order_kpi(
                order_id=50,
                customer_sk="customer-50",
                customer_id=150,
                order_total=450,
                units_sold=5,
            ),
        ],
        schema=_gold_order_kpis_schema(),
    )

    dim_customer_df = spark.createDataFrame(
        [
            (
                "customer-50",
                150,
                "Customer Fifty",
            ),
        ],
        schema=_dim_customer_schema(),
    )

    result = transform_gold_customer_sales(
        gold_order_kpis_df=order_kpis_df,
        dim_customer_df=dim_customer_df,
    )

    row = result.first()

    assert row.customer_sk == "customer-50"
    assert row.customer_id == 150
    assert row.customer_name == "Customer Fifty"
    assert row.order_count == 1
    assert row.sales_amount == 450
    assert row.units_sold == 5
    assert row.processed_timestamp is not None