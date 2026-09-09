# File: src/enterprise_api_ingestion_platform/framework/flatten_service.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer


class FlattenService:
    """Utility methods for flattening nested JSON structures."""

    @staticmethod
    def explode_array(
        dataframe: DataFrame,
        column_name: str,
    ) -> DataFrame:
        """
        Explodes an array column into multiple rows.

        Parameters
        ----------
        dataframe
            Input DataFrame.

        column_name
            Name of the array column.

        Returns
        -------
        DataFrame
            DataFrame with one row per array element.
        """
        return dataframe.withColumn(
            column_name,
            explode_outer(col(column_name)),
        )

    @staticmethod
    def flatten_struct(
        dataframe: DataFrame,
        column_name: str,
    ) -> DataFrame:
        """
        Flattens a struct column into top-level columns.

        Parameters
        ----------
        dataframe
            Input DataFrame.

        column_name
            Struct column to flatten.

        Returns
        -------
        DataFrame
            Flattened DataFrame.
        """
        struct_fields = dataframe.schema[column_name].dataType.fieldNames()

        return dataframe.select(
            "*",
            *[
                col(f"{column_name}.{field}").alias(field)
                for field in struct_fields
            ],
        ).drop(column_name)