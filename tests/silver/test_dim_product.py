# tests/silver/test_dim_product.py

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.silver.transformations import (
    transform_dim_product,
)


def _products_schema() -> StructType:
    """
    Returns the OpenAPI Products Bronze test schema.
    """

    rating_schema = StructType(
        [
            StructField("count", LongType(), True),
            StructField("rate", DoubleType(), True),
        ]
    )

    return StructType(
        [
            StructField("category", StringType(), True),
            StructField("description", StringType(), True),
            StructField("id", LongType(), True),
            StructField("image", StringType(), True),
            StructField("price", LongType(), True),
            StructField("rating", rating_schema, True),
            StructField("title", StringType(), True),
            StructField("_rescued_data", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_api_id", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )


def _product_row(
    product_id: int,
    title: str,
    ingestion_timestamp: datetime,
    source_file: str,
    price: int = 99,
) -> tuple:
    """
    Creates one OpenAPI Products Bronze test row.
    """

    return (
        "electronics",
        "Test product description",
        product_id,
        "https://example.com/product.jpg",
        price,
        {
            "count": 100,
            "rate": 4.5,
        },
        title,
        None,
        source_file,
        "openapi_products",
        ingestion_timestamp,
    )


def test_dim_product_transforms_product_attributes(spark):
    """
    Verifies product attributes and nested rating fields are flattened.
    """

    dataframe = spark.createDataFrame(
        [
            _product_row(
                product_id=1,
                title="Test Product",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="products-1.json",
                price=199,
            )
        ],
        schema=_products_schema(),
    )

    result = transform_dim_product(dataframe)

    row = result.first()

    assert row.product_id == 1
    assert row.title == "Test Product"
    assert row.description == "Test product description"
    assert row.category == "electronics"
    assert row.price == 199
    assert row.image == "https://example.com/product.jpg"
    assert row.rating_count == 100
    assert row.rating_rate == 4.5


def test_dim_product_generates_deterministic_surrogate_key(spark):
    """
    Verifies the same product ID produces the same surrogate key.
    """

    dataframe = spark.createDataFrame(
        [
            _product_row(
                product_id=42,
                title="Test Product",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="products-1.json",
            )
        ],
        schema=_products_schema(),
    )

    first_result = transform_dim_product(dataframe).first()
    second_result = transform_dim_product(dataframe).first()

    assert first_result.product_sk == second_result.product_sk
    assert first_result.product_sk is not None


def test_dim_product_keeps_latest_record(spark):
    """
    Verifies duplicate products resolve to the newest ingestion record.
    """

    dataframe = spark.createDataFrame(
        [
            _product_row(
                product_id=1,
                title="Old Product",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="products-old.json",
                price=99,
            ),
            _product_row(
                product_id=1,
                title="New Product",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    9,
                    0,
                    0,
                ),
                source_file="products-new.json",
                price=199,
            ),
        ],
        schema=_products_schema(),
    )

    result = transform_dim_product(dataframe)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].product_id == 1
    assert rows[0].title == "New Product"
    assert rows[0].price == 199


def test_dim_product_returns_one_row_per_product(spark):
    """
    Verifies duplicate source records produce unique product IDs.
    """

    dataframe = spark.createDataFrame(
        [
            _product_row(
                product_id=1,
                title="Product One",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="products-1.json",
            ),
            _product_row(
                product_id=1,
                title="Product One",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    9,
                    0,
                    0,
                ),
                source_file="products-2.json",
            ),
            _product_row(
                product_id=2,
                title="Product Two",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="products-1.json",
            ),
        ],
        schema=_products_schema(),
    )

    result = transform_dim_product(dataframe)

    assert result.count() == 2
    assert result.select("product_id").distinct().count() == 2