from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


class MergeUtils:
    """
    Reusable Delta Lake merge utilities.

    These methods are intentionally generic so they can be reused by any
    API/domain (Graph, GitHub, News API, Salesforce, ServiceNow, etc.).
    """

    @staticmethod
    def get_delta_table(
        spark,
        table_name: str,
    ) -> DeltaTable:
        """
        Return a DeltaTable instance.

        Args:
            spark: Active SparkSession.
            table_name: Fully-qualified Unity Catalog table.

        Returns:
            DeltaTable
        """
        return DeltaTable.forName(
            spark,
            table_name,
        )

    @staticmethod
    def build_merge(
        spark,
        source_df: DataFrame,
        target_table: str,
        business_key: str = "business_key",
    ):
        """
        Build a Delta MERGE statement.

        This method does not execute the merge.
        The caller decides whether to apply:
            - Type 1
            - Type 2
            - CDC
            - Custom merge logic

        Args:
            spark:
                Active SparkSession.

            source_df:
                Incoming DataFrame.

            target_table:
                Fully-qualified Unity Catalog target table.

            business_key:
                Merge key.

        Returns:
            DeltaMergeBuilder
        """

        target = MergeUtils.get_delta_table(
            spark,
            target_table,
        )

        return target.alias("target").merge(
            source_df.alias("source"),
            f"target.{business_key} = source.{business_key}",
        )

    @staticmethod
    def execute(builder) -> None:
        """
        Execute a prepared merge.
        """
        builder.execute()