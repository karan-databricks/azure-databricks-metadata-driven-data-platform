# src/enterprise_api_ingestion_platform/checkpoint/checkpoint_repository.py

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

from enterprise_api_ingestion_platform.checkpoint.checkpoint_model import (
    Checkpoint,
)
from enterprise_api_ingestion_platform.config.environment import environment


class CheckpointRepository:
    """
    Persists and retrieves API extraction checkpoints from Delta.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    def table_name(self) -> str:
        """
        Return the fully qualified checkpoint table name.
        """
        return (
            f"{environment.catalog}."
            f"{environment.metadata_schema}."
            "api_checkpoint"
        )

    def get_last_success_checkpoint(
        self,
        api_id: str,
    ) -> Checkpoint | None:
        """
        Return the most recently recorded checkpoint for an API.
        """
        try:
            row = (
                self.spark.table(self.table_name)
                .filter(col("api_id") == api_id)
                .orderBy(desc("recorded_at"))
                .limit(1)
                .collect()
            )

            if not row:
                return None

            checkpoint = row[0]

            return Checkpoint(
                api_id=checkpoint["api_id"],
                state_type=checkpoint["state_type"],
                state_value=checkpoint["state_value"],
                recorded_at=checkpoint["recorded_at"],
                run_id=checkpoint["run_id"],
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to retrieve checkpoint for API '{api_id}'."
            ) from exc

    def save_checkpoint(
        self,
        api_id: str,
        state_type: str,
        state_value: str,
        recorded_at: datetime,
        run_id: str,
    ) -> None:
        """
        Persist a successfully completed extraction checkpoint.
        """
        try:
            dataframe = self.spark.createDataFrame(
                [
                    (
                        api_id,
                        state_type,
                        state_value,
                        recorded_at,
                        run_id,
                    )
                ],
                schema="""
                    api_id STRING,
                    state_type STRING,
                    state_value STRING,
                    recorded_at TIMESTAMP,
                    run_id STRING
                """,
            )

            (
                dataframe.write
                .format("delta")
                .mode("append")
                .saveAsTable(self.table_name)
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to save checkpoint for API '{api_id}'."
            ) from exc