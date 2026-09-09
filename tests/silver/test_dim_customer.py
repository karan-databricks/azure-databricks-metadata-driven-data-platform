# tests/silver/test_dim_customer.py

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
    transform_dim_customer,
)


def _users_schema() -> StructType:
    """
    Returns the OpenAPI Users Bronze test schema.
    """

    geo_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
        ]
    )

    address_schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("geo", geo_schema, True),
            StructField("street", StringType(), True),
            StructField("suite", StringType(), True),
            StructField("zipcode", StringType(), True),
        ]
    )

    company_schema = StructType(
        [
            StructField("bs", StringType(), True),
            StructField("catchPhrase", StringType(), True),
            StructField("name", StringType(), True),
        ]
    )

    return StructType(
        [
            StructField("address", address_schema, True),
            StructField("avatar", StringType(), True),
            StructField("company", company_schema, True),
            StructField("email", StringType(), True),
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("role", StringType(), True),
            StructField("username", StringType(), True),
            StructField("website", StringType(), True),
            StructField("_rescued_data", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_api_id", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )


def _user_row(
    user_id: int,
    name: str,
    ingestion_timestamp: datetime,
    source_file: str,
) -> tuple:
    """
    Creates one OpenAPI Users Bronze test row.
    """

    return (
        {
            "city": "Austin",
            "geo": {
                "lat": 30.2672,
                "lng": -97.7431,
            },
            "street": "Main Street",
            "suite": "Suite 100",
            "zipcode": "78701",
        },
        "avatar.jpg",
        {
            "bs": "test business",
            "catchPhrase": "Test phrase",
            "name": "Test Company",
        },
        f"{name.lower().replace(' ', '.')}@example.com",
        user_id,
        name,
        "555-0100",
        "user",
        name.lower().replace(" ", "_"),
        "example.com",
        None,
        source_file,
        "openapi_users",
        ingestion_timestamp,
    )


def test_dim_customer_flattens_nested_attributes(spark):
    """
    Verifies nested address and company fields are flattened.
    """

    dataframe = spark.createDataFrame(
        [
            _user_row(
                user_id=1,
                name="Alice Smith",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="users-1.json",
            )
        ],
        schema=_users_schema(),
    )

    result = transform_dim_customer(dataframe)

    row = result.first()

    assert row.customer_id == 1
    assert row.customer_name == "Alice Smith"
    assert row.address_street == "Main Street"
    assert row.address_suite == "Suite 100"
    assert row.address_city == "Austin"
    assert row.address_zipcode == "78701"
    assert row.address_lat == 30.2672
    assert row.address_lng == -97.7431
    assert row.company_name == "Test Company"
    assert row.company_catch_phrase == "Test phrase"
    assert row.company_bs == "test business"


def test_dim_customer_generates_deterministic_surrogate_key(spark):
    """
    Verifies the same customer ID produces the same surrogate key.
    """

    dataframe = spark.createDataFrame(
        [
            _user_row(
                user_id=42,
                name="Alice Smith",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="users-1.json",
            )
        ],
        schema=_users_schema(),
    )

    first_result = transform_dim_customer(dataframe).first()
    second_result = transform_dim_customer(dataframe).first()

    assert first_result.customer_sk == second_result.customer_sk
    assert first_result.customer_sk is not None


def test_dim_customer_keeps_latest_record(spark):
    """
    Verifies duplicate customers resolve to the newest ingestion record.
    """

    dataframe = spark.createDataFrame(
        [
            _user_row(
                user_id=1,
                name="Old Name",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="users-old.json",
            ),
            _user_row(
                user_id=1,
                name="New Name",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    9,
                    0,
                    0,
                ),
                source_file="users-new.json",
            ),
        ],
        schema=_users_schema(),
    )

    result = transform_dim_customer(dataframe)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].customer_id == 1
    assert rows[0].customer_name == "New Name"


def test_dim_customer_returns_one_row_per_customer(spark):
    """
    Verifies duplicate source records produce unique customer IDs.
    """

    dataframe = spark.createDataFrame(
        [
            _user_row(
                user_id=1,
                name="Alice Smith",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="users-1.json",
            ),
            _user_row(
                user_id=1,
                name="Alice Smith",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    9,
                    0,
                    0,
                ),
                source_file="users-2.json",
            ),
            _user_row(
                user_id=2,
                name="Bob Jones",
                ingestion_timestamp=datetime(
                    2026,
                    8,
                    28,
                    8,
                    0,
                    0,
                ),
                source_file="users-1.json",
            ),
        ],
        schema=_users_schema(),
    )

    result = transform_dim_customer(dataframe)

    assert result.count() == 2
    assert result.select("customer_id").distinct().count() == 2