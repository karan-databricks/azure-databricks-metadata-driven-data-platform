from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class SchemaRegistry:
    """Central registry for all API schemas."""

    @staticmethod
    def graph_users() -> StructType:
        return StructType(
            [
                StructField("@odata.context", StringType(), True),
                StructField(
                    "value",
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    "businessPhones",
                                    ArrayType(StringType()),
                                    True,
                                ),
                                StructField(
                                    "displayName",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "givenName",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "id",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "jobTitle",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "mail",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "mobilePhone",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "officeLocation",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "preferredLanguage",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "surname",
                                    StringType(),
                                    True,
                                ),
                                StructField(
                                    "userPrincipalName",
                                    StringType(),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        
    @staticmethod
    def newsapi_top_headlines() -> StructType:
        return StructType(
            [
                StructField(
                    "articles",
                    ArrayType(
                        StructType(
                            [
                                StructField("author", StringType(), True),
                                StructField("content", StringType(), True),
                                StructField("description", StringType(), True),
                                StructField("publishedAt", StringType(), True),
                                StructField(
                                    "source",
                                    StructType(
                                        [
                                            StructField("id", StringType(), True),
                                            StructField("name", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField("title", StringType(), True),
                                StructField("url", StringType(), True),
                                StructField("urlToImage", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField("status", StringType(), True),
                StructField("totalResults", LongType(), True),
                StructField("ingestion_date", DateType(), True),
            ]
        )