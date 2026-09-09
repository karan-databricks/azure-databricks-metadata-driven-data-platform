from pyspark.sql import DataFrame

from enterprise_api_ingestion_platform.framework.flatten_service import (
    FlattenService,
)


class GenericTransform:
    """Reusable transformations for Silver pipelines."""

    @staticmethod
    def graph_users(dataframe: DataFrame) -> DataFrame:
        dataframe = FlattenService.explode_array(
            dataframe,
            "value",
        )

        dataframe = FlattenService.flatten_struct(
            dataframe,
            "value",
        )

        dataframe = dataframe.withColumn(
            "business_key",
            dataframe.id,
        )

        dataframe = dataframe.dropDuplicates(
            ["business_key"]
        )

        return dataframe