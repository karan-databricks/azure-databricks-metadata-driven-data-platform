# tests/silver/test_fact_order.py

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.silver.transformations import (
    transform_fact_order,
)


def _orders_schema() -> StructType:
    """Return the schema used by the order transformation tests."""
    return StructType(
        [
            StructField("date", StringType(), True),
            StructField("id", LongType(), True),
            StructField(
                "products",
                ArrayType(
                    StructType(
                        [
                            StructField("productId", LongType(), True),
                            StructField("quantity", LongType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField("status", StringType(), True),
            StructField("total", LongType(), True),
            StructField("userId", LongType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_api_id", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )


def _order_row(
    order_id: int,
    customer_id: int,
    products: list[dict[str, int]],
    ingestion_timestamp: datetime,
    source_file: str,
    total: int,
    business_date: str,
    status: str = "delivered",
) -> dict:
    """Build an OpenAPI order fixture row."""
    return {
        "date": business_date,
        "id": order_id,
        "products": products,
        "status": status,
        "total": total,
        "userId": customer_id,
        "_source_file": source_file,
        "_api_id": "openapi_orders",
        "ingestion_timestamp": ingestion_timestamp,
    }


def _customer_df(spark):
    """Build a customer dimension fixture."""
    return spark.createDataFrame(
        [
            Row(
                customer_sk="customer-sk-1",
                customer_id=71,
            ),
            Row(
                customer_sk="customer-sk-2",
                customer_id=81,
            ),
        ]
    )


def _product_df(spark):
    """Build a product dimension fixture."""
    return spark.createDataFrame(
        [
            Row(
                product_sk="product-sk-85",
                product_id=85,
            ),
            Row(
                product_sk="product-sk-169",
                product_id=169,
            ),
        ]
    )


def test_fact_order_explodes_product_lines(spark):
    """Verify one order with multiple products becomes multiple fact rows."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                    {"productId": 169, "quantity": 2},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    rows = result.orderBy("line_number").collect()

    assert len(rows) == 2
    assert rows[0].order_id == 1
    assert rows[0].line_number == 1
    assert rows[0].product_id == 85
    assert rows[0].quantity == 1
    assert rows[1].order_id == 1
    assert rows[1].line_number == 2
    assert rows[1].product_id == 169
    assert rows[1].quantity == 2


def test_fact_order_assigns_customer_surrogate_key(spark):
    """Verify the customer surrogate key is resolved from the dimension."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    row = result.collect()[0]

    assert row.customer_id == 71
    assert row.customer_sk == "customer-sk-1"


def test_fact_order_assigns_product_surrogate_key(spark):
    """Verify the product surrogate key is resolved from the dimension."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    row = result.collect()[0]

    assert row.product_id == 85
    assert row.product_sk == "product-sk-85"


def test_fact_order_generates_deterministic_surrogate_key(spark):
    """Verify the order surrogate key is deterministic."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result_one = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    result_two = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    assert result_one.collect()[0].order_sk == result_two.collect()[0].order_sk


def test_fact_order_deduplicates_latest_complete_order_snapshot(spark):
    """Verify the latest complete snapshot is selected before exploding lines."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders-old.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 169, "quantity": 3},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    11,
                    0,
                    0,
                ),
                source_file="orders-new.json",
                total=300,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].product_id == 169
    assert rows[0].quantity == 3
    assert rows[0].order_total == 300


def test_fact_order_deduplicates_using_source_file_as_tiebreaker(spark):
    """Verify source file deterministically breaks ingestion timestamp ties."""
    ingestion_timestamp = datetime(
        2026,
        8,
        28,
        10,
        0,
        0,
    )

    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=ingestion_timestamp,
                source_file="orders-a.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 169, "quantity": 2},
                ],
                ingestion_timestamp=ingestion_timestamp,
                source_file="orders-z.json",
                total=200,
                business_date="2025-12-19T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].product_id == 169
    assert rows[0].quantity == 2
    assert rows[0].order_total == 200


def test_fact_order_keeps_multiple_orders_with_distinct_business_keys(spark):
    """Verify distinct order IDs are retained."""
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders-1.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
            _order_row(
                order_id=2,
                customer_id=81,
                products=[
                    {"productId": 169, "quantity": 2},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    11,
                    0,
                    0,
                ),
                source_file="orders-2.json",
                total=200,
                business_date="2025-12-18T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    rows = result.orderBy("order_id").collect()

    assert len(rows) == 2
    assert [row.order_id for row in rows] == [1, 2]


def test_fact_order_keeps_late_arriving_order_with_older_business_date(
    spark,
):
    """
    Verifies a late-arriving order is retained even when its business date
    is older than an already-ingested order.
    """
    bronze_df = spark.createDataFrame(
        [
            _order_row(
                order_id=1,
                customer_id=71,
                products=[
                    {"productId": 85, "quantity": 1},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    10,
                    0,
                    0,
                ),
                source_file="orders-current.json",
                total=100,
                business_date="2025-12-19T04:52:57.301Z",
            ),
            _order_row(
                order_id=2,
                customer_id=81,
                products=[
                    {"productId": 169, "quantity": 2},
                ],
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    11,
                    0,
                    0,
                ),
                source_file="orders-late.json",
                total=200,
                business_date="2025-12-18T04:52:57.301Z",
            ),
        ],
        schema=_orders_schema(),
    )

    result = transform_fact_order(
        bronze_df=bronze_df,
        customer_df=_customer_df(spark),
        product_df=_product_df(spark),
    )

    rows = (
        result.orderBy("order_id")
        .select(
            "order_id",
            F.date_format(
                "order_date",
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ).alias("order_date_utc"),
        )
        .collect()
    )

    assert len(rows) == 2

    assert rows[0].order_id == 1
    assert rows[0].order_date_utc == "2025-12-19T04:52:57.301Z"

    assert rows[1].order_id == 2
    assert rows[1].order_date_utc == "2025-12-18T04:52:57.301Z"