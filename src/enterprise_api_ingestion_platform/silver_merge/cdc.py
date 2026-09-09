from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


@dataclass
class CDCResult:
    """
    Holds the outcome of change detection.
    """

    new_records: DataFrame
    changed_records: DataFrame
    unchanged_records: DataFrame


class CDC:
    """
    Generic Change Data Capture framework.

    Compares an incoming Silver DataFrame with an existing target
    DataFrame using business_key and record_hash.

    This class does not perform MERGE operations.
    It only classifies records.
    """

    @staticmethod
    def detect_changes(
        source_df: DataFrame,
        target_df: DataFrame,
    ) -> CDCResult:
        """
        Detect new, changed and unchanged records.

        Parameters
        ----------
        source_df
            Incoming Silver DataFrame.

        target_df
            Existing target DataFrame.

        Returns
        -------
        CDCResult
        """

        source = source_df.alias("source")
        target = target_df.alias("target")

        new_records = (
            source.join(
                target,
                on="business_key",
                how="left_anti",
            )
        )

        changed_records = (
            source.join(
                target,
                on="business_key",
                how="inner",
            )
            .filter(
                col("source.record_hash")
                != col("target.record_hash")
            )
            .select("source.*")
        )

        unchanged_records = (
            source.join(
                target,
                on="business_key",
                how="inner",
            )
            .filter(
                col("source.record_hash")
                == col("target.record_hash")
            )
            .select("source.*")
        )

        return CDCResult(
            new_records=new_records,
            changed_records=changed_records,
            unchanged_records=unchanged_records,
        )