from enterprise_api_ingestion_platform.config.environment import environment
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from enterprise_api_ingestion_platform.common.exceptions import (
    AuditLoggingException,
)


class AuditLogger:
    """
    Writes execution audit records to the audit Delta table.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def log_execution(
        self,
        run_id: str,
        api_name: str,
        job_name: str,
        start_time,
        end_time,
        status: str,
        records_read: int | None,
        records_written: int | None,
        duration_seconds: float,
        error_message: str | None = None,
        landing_file: str | None = None,
    ) -> None:

        schema = StructType(
            [
                StructField("run_id", StringType(), False),
                StructField("api_name", StringType(), False),
                StructField("job_name", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), False),
                StructField("status", StringType(), False),
                StructField("records_read", IntegerType(), True),
                StructField("records_written", IntegerType(), True),
                StructField("duration_seconds", DoubleType(), False),
                StructField("error_message", StringType(), True),
                StructField("landing_file", StringType(), True),
            ]
        )

        try:

            dataframe = self.spark.createDataFrame(
                [
                    (
                        run_id,
                        api_name,
                        job_name,
                        start_time,
                        end_time,
                        status,
                        records_read,
                        records_written,
                        duration_seconds,
                        error_message,
                        landing_file,
                    )
                ],
                schema=schema,
            )

            (
                dataframe.write
                .format("delta")
                .mode("append")
                .saveAsTable(
                    f"{environment.catalog}."
                    f"{environment.audit_schema}."
                    "job_execution"
                )
            )

        except Exception as exc:
            raise AuditLoggingException(
                f"Failed to write audit log: {exc}"
            ) from exc