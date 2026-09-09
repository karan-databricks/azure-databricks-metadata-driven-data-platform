from __future__ import annotations

from pyspark.sql import DataFrame

from enterprise_api_ingestion_platform.silver_merge.merge_utils import (
    MergeUtils,
)


class SCDType1:
    """
    Generic Slowly Changing Dimension Type 1 merge.

    Existing rows are overwritten.
    New rows are inserted.

    No history is preserved.
    """

    @staticmethod
    def merge(
        spark,
        source_df: DataFrame,
        target_table: str,
        business_key: str = "business_key",
    ) -> None:
        """
        Execute a generic SCD Type 1 merge.

        Args:
            spark:
                Active SparkSession.

            source_df:
                Incoming Silver DataFrame.

            target_table:
                Fully-qualified Unity Catalog table.

            business_key:
                Primary merge key.
        """

        (
            MergeUtils.build_merge(
                spark=spark,
                source_df=source_df,
                target_table=target_table,
                business_key=business_key,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )