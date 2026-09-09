# tests/gold/test_gold_product_sales.py

from datetime import datetime

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.gold.transformations import (
    transform_gold_product_sales,
)


def _fact_order_schema() -> StructType:
    """
    Returns the minimal production-compatible fact_order schema
    required by the transformation.
    """

    return StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("line_number", LongType(), False),
            StructField("product_sk", StringType(), False),
            StructField("product_id", LongType(), False),
            StructField("quantity", LongType(), True),
            StructField("order_date", TimestampType(), True),
        ]
    )


def _dim_product_schema() -> StructType:
    """
    Returns the minimal production-compatible dim_product schema
    required by the transformation.
    """

    return StructType(
        [
            StructField("product_sk", StringType(), False),
            StructField("product_id", LongType(), False),
            StructField("title", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )


def _order_line(
    order_id: int,
    line_number: int,
    product_sk: str,
    product_id: int,
    quantity: int,
    order_date: datetime,
) -> tuple:
    """Creates one fact_order test row."""

    return (
        order_id,
        line_number,
        product_sk,
        product_id,
        quantity,
        order_date,
    )


def _product(
    product_sk: str,
    product_id: int,
    title: str,
    category: str,
) -> tuple:
    """Creates one dim_product test row."""

    return (
        product_sk,
        product_id,
        title,
        category,
    )


def test_gold_product_sales_aggregates_product_orders(
    spark,
) -> None:
    """
    Verifies multiple order lines for a product aggregate correctly.
    """

    fact_order_df = spark.createDataFrame(
        [
            _order_line(
                order_id=100,
                line_number=1,
                product_sk="product-1",
                product_id=10,
                quantity=2,
                order_date=datetime(2026, 8, 27, 10, 0, 0),
            ),
            _order_line(
                order_id=101,
                line_number=1,
                product_sk="product-1",
                product_id=10,
                quantity=3,
                order_date=datetime(2026, 8, 28, 11, 0, 0),
            ),
        ],
        schema=_fact_order_schema(),
    )

    dim_product_df = spark.createDataFrame(
        [
            _product(
                product_sk="product-1",
                product_id=10,
                title="Product A",
                category="Category A",
            ),
        ],
        schema=_dim_product_schema(),
    )

    result = transform_gold_product_sales(
        fact_order_df,
        dim_product_df,
    )

    row = result.first()

    assert row.product_id == 10
    assert row.product_title == "Product A"
    assert row.category == "Category A"
    assert row.order_count == 2
    assert row.units_sold == 5
    assert row.first_order_date == datetime(
        2026,
        8,
        27,
        10,
        0,
        0,
    )
    assert row.last_order_date == datetime(
        2026,
        8,
        28,
        11,
        0,
        0,
    )


def test_gold_product_sales_counts_distinct_orders(
    spark,
) -> None:
    """
    Verifies multiple lines from one order count as one order.
    """

    fact_order_df = spark.createDataFrame(
        [
            _order_line(
                order_id=200,
                line_number=1,
                product_sk="product-2",
                product_id=20,
                quantity=1,
                order_date=datetime(2026, 8, 28, 10, 0, 0),
            ),
            _order_line(
                order_id=200,
                line_number=2,
                product_sk="product-2",
                product_id=20,
                quantity=4,
                order_date=datetime(2026, 8, 28, 10, 0, 0),
            ),
            _order_line(
                order_id=201,
                line_number=1,
                product_sk="product-2",
                product_id=20,
                quantity=2,
                order_date=datetime(2026, 8, 29, 12, 0, 0),
            ),
        ],
        schema=_fact_order_schema(),
    )

    dim_product_df = spark.createDataFrame(
        [
            _product(
                product_sk="product-2",
                product_id=20,
                title="Product B",
                category="Category B",
            ),
        ],
        schema=_dim_product_schema(),
    )

    result = transform_gold_product_sales(
        fact_order_df,
        dim_product_df,
    )

    row = result.first()

    assert row.order_count == 2
    assert row.units_sold == 7


def test_gold_product_sales_returns_one_row_per_product(
    spark,
) -> None:
    """
    Verifies each product produces exactly one Gold row.
    """

    fact_order_df = spark.createDataFrame(
        [
            _order_line(
                order_id=300,
                line_number=1,
                product_sk="product-1",
                product_id=10,
                quantity=2,
                order_date=datetime(2026, 8, 28, 10, 0, 0),
            ),
            _order_line(
                order_id=301,
                line_number=1,
                product_sk="product-2",
                product_id=20,
                quantity=3,
                order_date=datetime(2026, 8, 28, 11, 0, 0),
            ),
            _order_line(
                order_id=302,
                line_number=1,
                product_sk="product-3",
                product_id=30,
                quantity=4,
                order_date=datetime(2026, 8, 29, 12, 0, 0),
            ),
        ],
        schema=_fact_order_schema(),
    )

    dim_product_df = spark.createDataFrame(
        [
            _product(
                product_sk="product-1",
                product_id=10,
                title="Product A",
                category="Category A",
            ),
            _product(
                product_sk="product-2",
                product_id=20,
                title="Product B",
                category="Category B",
            ),
            _product(
                product_sk="product-3",
                product_id=30,
                title="Product C",
                category="Category C",
            ),
        ],
        schema=_dim_product_schema(),
    )

    result = transform_gold_product_sales(
        fact_order_df,
        dim_product_df,
    )

    assert result.count() == 3
    assert result.select("product_id").distinct().count() == 3


def test_gold_product_sales_preserves_product_identity_and_attributes(
    spark,
) -> None:
    """
    Verifies product surrogate keys and dimension attributes are preserved.
    """

    fact_order_df = spark.createDataFrame(
        [
            _order_line(
                order_id=400,
                line_number=1,
                product_sk="product-42",
                product_id=42,
                quantity=6,
                order_date=datetime(2026, 8, 29, 14, 0, 0),
            ),
        ],
        schema=_fact_order_schema(),
    )

    dim_product_df = spark.createDataFrame(
        [
            _product(
                product_sk="product-42",
                product_id=42,
                title="Test Product",
                category="Test Category",
            ),
        ],
        schema=_dim_product_schema(),
    )

    result = transform_gold_product_sales(
        fact_order_df,
        dim_product_df,
    )

    row = result.first()

    assert row.product_sk == "product-42"
    assert row.product_id == 42
    assert row.product_title == "Test Product"
    assert row.category == "Test Category"
    assert row.order_count == 1
    assert row.units_sold == 6
    assert row.first_order_date == datetime(
        2026,
        8,
        29,
        14,
        0,
        0,
    )
    assert row.last_order_date == datetime(
        2026,
        8,
        29,
        14,
        0,
        0,
    )