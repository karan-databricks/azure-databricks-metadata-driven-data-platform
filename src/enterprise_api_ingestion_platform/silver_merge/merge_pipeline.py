from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import DataFrame, SparkSession

from enterprise_api_ingestion_platform.silver_merge.cdc import CDC
from enterprise_api_ingestion_platform.silver_merge.scd_type1 import SCDType1
from enterprise_api_ingestion_platform.silver_merge.scd_type2 import SCDType2


@dataclass
class MergeMetrics:
    """
    Metrics collected during merge execution.
    """

    source_rows: int
    target_rows: int
    new_rows: int
    changed_rows: int
    unchanged_rows: int


class MergeStrategy(Enum):
    """
    Supported merge strategies.
    """

    TYPE1 = "type1"
    TYPE2 = "type2"


class MergePipeline:
    """
    Enterprise merge orchestration pipeline.

    Responsibilities
    ----------------
    1. Load source table
    2. Load target table
    3. Perform CDC
    4. Execute merge strategy
    5. Return execution metrics

    No Delta MERGE logic exists in this class.
    """

    @staticmethod
    def load_source(
        spark: SparkSession,
        table_name: str,
    ) -> DataFrame:
        """
        Load the source table.
        """
        return spark.table(table_name)

    @staticmethod
    def load_target(
        spark: SparkSession,
        table_name: str,
    ) -> DataFrame:
        """
        Load the target table.
        """
        return spark.table(table_name)

    @staticmethod
    def collect_metrics(
        source_df: DataFrame,
        target_df: DataFrame,
        cdc_result,
    ) -> MergeMetrics:
        """
        Collect merge execution metrics.
        """

        return MergeMetrics(
            source_rows=source_df.count(),
            target_rows=target_df.count(),
            new_rows=cdc_result.new_records.count(),
            changed_rows=cdc_result.changed_records.count(),
            unchanged_rows=cdc_result.unchanged_records.count(),
        )

    @staticmethod
    def execute_merge(
        spark: SparkSession,
        cdc_result,
        target_table: str,
        strategy: MergeStrategy,
        business_key: str = "business_key",
    ) -> None:
        """
        Execute the selected merge strategy.
        """

        merge_source = cdc_result.new_records.unionByName(
            cdc_result.changed_records
        )

        strategy_map = {
            MergeStrategy.TYPE1: SCDType1,
            MergeStrategy.TYPE2: SCDType2,
        }

        merge_class = strategy_map.get(strategy)

        if merge_class is None:
            raise ValueError(
                f"Unsupported merge strategy: {strategy}"
            )

        merge_class.merge(
            spark=spark,
            source_df=merge_source,
            target_table=target_table,
            business_key=business_key,
        )

    @staticmethod
    def run(
        spark: SparkSession,
        source_table: str,
        target_table: str,
        strategy: MergeStrategy = MergeStrategy.TYPE1,
        business_key: str = "business_key",
    ) -> MergeMetrics:
        """
        Execute a complete merge workflow.

        Parameters
        ----------
        spark
            Active Spark session.

        source_table
            Fully-qualified Silver table.

        target_table
            Fully-qualified target Delta table.

        strategy
            Merge strategy.

        business_key
            Merge key.

        Returns
        -------
        MergeMetrics
        """

        source_df = MergePipeline.load_source(
            spark,
            source_table,
        )

        target_df = MergePipeline.load_target(
            spark,
            target_table,
        )

        cdc_result = CDC.detect_changes(
            source_df=source_df,
            target_df=target_df,
        )

        MergePipeline.execute_merge(
            spark=spark,
            cdc_result=cdc_result,
            target_table=target_table,
            strategy=strategy,
            business_key=business_key,
        )

        return MergePipeline.collect_metrics(
            source_df,
            target_df,
            cdc_result,
        )