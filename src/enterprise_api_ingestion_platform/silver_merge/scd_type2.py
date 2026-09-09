from __future__ import annotations

from pyspark.sql import DataFrame

from enterprise_api_ingestion_platform.silver_merge.merge_utils import (
    MergeUtils,
)


class SCDType2:
    """
    Generic Slowly Changing Dimension Type 2 merge.

    This implementation provides the merge framework used for
    historical versioning.

    NOTE:
    The source DataFrame must already contain the SCD columns:

        effective_from
        effective_to
        is_current

    A later pipeline step will populate those columns before invoking
    this merge.
    """

    @staticmethod
    def merge(
        spark,
        source_df: DataFrame,
        target_table: str,
        business_key: str = "business_key",
    ) -> None:
        """
        Execute a generic SCD Type 2 merge.

        Existing current records are updated.

        New records are inserted.
        """

        (
            MergeUtils.build_merge(
                spark=spark,
                source_df=source_df,
                target_table=target_table,
                business_key=business_key,
            )
            .whenMatchedUpdate(
                condition=(
                    "target.record_hash <> source.record_hash "
                    "AND target.is_current = true"
                ),
                set={
                    "effective_to": "source.effective_from",
                    "is_current": "false",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )