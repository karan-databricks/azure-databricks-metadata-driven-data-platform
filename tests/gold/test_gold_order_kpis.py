# ============================================================
# FILE:
# tests/gold/test_gold_order_kpis.py
# ============================================================

from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.gold.transformations import (
    transform_gold_order_kpis,
)


def _fact_order_schema() -> StructType:
    """
    Returns the production-compatible fact_order test schema.
    """

    return StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("line_number", LongType(), False),
            StructField("customer_sk", StringType(), True),
            StructField("customer_id", LongType(), True),
            StructField("product_sk", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("quantity", LongType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("order_total", LongType(), True),
            StructField(
                "ingestion_timestamp",
                TimestampType(),
                True,
            ),
            StructField(
                "processed_timestamp",
                TimestampType(),
                True,
            ),
        ]
    )


def _order_line(
    order_id: int,
    line_number: int,
    customer_sk: str,
    customer_id: int,
    product_sk: str,
    product_id: int,
    quantity: int,
    order_date: datetime,
    status: str,
    order_total: int,
) -> tuple:
    """
    Creates one production-compatible fact_order test row.
    """

    return (
        order_id,
        line_number,
        customer_sk,
        customer_id,
        product_sk,
        product_id,
        quantity,
        order_date,
        status,
        order_total,
        datetime(
            2026,
            8,
            29,
            10,
            0,
            0,
        ),
        datetime(
            2026,
            8,
            29,
            10,
            5,
            0,
        ),
    )


def test_gold_order_kpis_aggregates_one_order(spark) -> None:
    """
    Verifies multiple order lines become one order-level row.
    """

    order_date = datetime(
        2025,
        12,
        19,
        4,
        52,
        57,
    )

    dataframe = spark.createDataFrame(
        [
            _order_line(
                order_id=1,
                line_number=1,
                customer_sk="customer-1",
                customer_id=71,
                product_sk="product-1",
                product_id=85,
                quantity=1,
                order_date=order_date,
                status="delivered",
                order_total=1082,
            ),
            _order_line(
                order_id=1,
                line_number=2,
                customer_sk="customer-1",
                customer_id=71,
                product_sk="product-2",
                product_id=169,
                quantity=2,
                order_date=order_date,
                status="delivered",
                order_total=1082,
            ),
            _order_line(
                order_id=1,
                line_number=3,
                customer_sk="customer-1",
                customer_id=71,
                product_sk="product-3",
                product_id=19,
                quantity=2,
                order_date=order_date,
                status="delivered",
                order_total=1082,
            ),
        ],
        schema=_fact_order_schema(),
    )

    result = transform_gold_order_kpis(dataframe)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].order_id == 1
    assert rows[0].customer_sk == "customer-1"
    assert rows[0].customer_id == 71
    assert rows[0].order_date == order_date
    assert rows[0].status == "delivered"
    assert rows[0].order_total == 1082
    assert rows[0].line_count == 3
    assert rows[0].units_sold == 5
    assert rows[0].processed_timestamp is not None


def test_gold_order_kpis_does_not_sum_order_total(spark) -> None:
    """
    Verifies an order-level total is not multiplied by line count.
    """

    order_date = datetime(
        2025,
        12,
        18,
        20,
        20,
        34,
    )

    dataframe = spark.createDataFrame(
        [
            _order_line(
                order_id=2,
                line_number=1,
                customer_sk="customer-2",
                customer_id=81,
                product_sk="product-50",
                product_id=50,
                quantity=3,
                order_date=order_date,
                status="cancelled",
                order_total=589,
            ),
            _order_line(
                order_id=2,
                line_number=2,
                customer_sk="customer-2",
                customer_id=81,
                product_sk="product-138",
                product_id=138,
                quantity=2,
                order_date=order_date,
                status="cancelled",
                order_total=589,
            ),
        ],
        schema=_fact_order_schema(),
    )

    result = transform_gold_order_kpis(dataframe)

    row = result.first()

    assert row.order_total == 589
    assert row.order_total != 1178
    assert row.units_sold == 5


def test_gold_order_kpis_returns_one_row_per_order(spark) -> None:
    """
    Verifies the Gold output grain is one row per order.
    """

    dataframe = spark.createDataFrame(
        [
            _order_line(
                order_id=1,
                line_number=1,
                customer_sk="customer-1",
                customer_id=71,
                product_sk="product-1",
                product_id=85,
                quantity=1,
                order_date=datetime(
                    2025,
                    12,
                    19,
                    4,
                    52,
                    57,
                ),
                status="delivered",
                order_total=1082,
            ),
            _order_line(
                order_id=1,
                line_number=2,
                customer_sk="customer-1",
                customer_id=71,
                product_sk="product-2",
                product_id=169,
                quantity=2,
                order_date=datetime(
                    2025,
                    12,
                    19,
                    4,
                    52,
                    57,
                ),
                status="delivered",
                order_total=1082,
            ),
            _order_line(
                order_id=2,
                line_number=1,
                customer_sk="customer-2",
                customer_id=81,
                product_sk="product-50",
                product_id=50,
                quantity=3,
                order_date=datetime(
                    2025,
                    12,
                    18,
                    20,
                    20,
                    34,
                ),
                status="cancelled",
                order_total=589,
            ),
            _order_line(
                order_id=2,
                line_number=2,
                customer_sk="customer-2",
                customer_id=81,
                product_sk="product-138",
                product_id=138,
                quantity=2,
                order_date=datetime(
                    2025,
                    12,
                    18,
                    20,
                    20,
                    34,
                ),
                status="cancelled",
                order_total=589,
            ),
        ],
        schema=_fact_order_schema(),
    )

    result = transform_gold_order_kpis(dataframe)

    assert result.count() == 2
    assert result.select("order_id").distinct().count() == 2


def test_gold_order_kpis_preserves_order_attributes(spark) -> None:
    """
    Verifies customer, status, date, and order total remain associated
    with the correct order.
    """

    order_date = datetime(
        2025,
        12,
        20,
        10,
        30,
        0,
    )

    dataframe = spark.createDataFrame(
        [
            _order_line(
                order_id=50,
                line_number=1,
                customer_sk="customer-50",
                customer_id=150,
                product_sk="product-50",
                product_id=250,
                quantity=2,
                order_date=order_date,
                status="shipped",
                order_total=450,
            ),
            _order_line(
                order_id=50,
                line_number=2,
                customer_sk="customer-50",
                customer_id=150,
                product_sk="product-51",
                product_id=251,
                quantity=3,
                order_date=order_date,
                status="shipped",
                order_total=450,
            ),
        ],
        schema=_fact_order_schema(),
    )

    result = transform_gold_order_kpis(dataframe)

    row = result.first()

    assert row.order_id == 50
    assert row.customer_sk == "customer-50"
    assert row.customer_id == 150
    assert row.order_date == order_date
    assert row.status == "shipped"
    assert row.order_total == 450
    assert row.line_count == 2
    assert row.units_sold == 5
