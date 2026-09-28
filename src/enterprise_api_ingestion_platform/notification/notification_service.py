# src/enterprise_api_ingestion_platform/notification/notification_service.py

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NotificationSummary:
    """
    Represents the operational summary for one API ingestion job run.
    """

    job_run_id: str
    job_name: str
    overall_status: str

    api_records_read: int | None
    pages_read: int | None
    landing_file: str | None
    landing_file_size_bytes: int | None

    ingestion_status: str
    bronze_status: str
    silver_status: str
    gold_status: str
    dq_status: str

    reconciliation_status: str

    failure_stage: str | None
    failure_type: str | None
    retry_count: int | None
    total_attempts: int | None
    error_message: str | None


class NotificationService:
    """
    Builds the operational notification content for an ingestion job run.

    Delivery is intentionally not implemented here yet. This service is
    responsible only for converting execution information into a stable
    notification representation.
    """

    def build_subject(
        self,
        summary: NotificationSummary,
    ) -> str:
        """
        Builds the notification email subject.
        """

        return (
            f"API Ingestion Run Summary - "
            f"{summary.overall_status} - "
            f"{summary.job_run_id}"
        )

    def build_body(
        self,
        summary: NotificationSummary,
    ) -> str:
        """
        Builds the plain-text operational notification body.
        """

        lines = [
            f"Job: {summary.job_name}",
            f"Run ID: {summary.job_run_id}",
            f"Overall Status: {summary.overall_status}",
            "",
            "Ingestion",
            "---------",
            f"API records read: {self._format_value(summary.api_records_read)}",
            f"Pages read: {self._format_value(summary.pages_read)}",
            f"Landing file: {self._format_value(summary.landing_file)}",
            (
                "Landing file size (bytes): "
                f"{self._format_value(summary.landing_file_size_bytes)}"
            ),
            f"Ingestion status: {summary.ingestion_status}",
            "",
            "Pipeline",
            "--------",
            f"Bronze: {summary.bronze_status}",
            f"Silver: {summary.silver_status}",
            f"Gold: {summary.gold_status}",
            "",
            "Data Quality",
            "-----------",
            f"DQ status: {summary.dq_status}",
            "",
            "Reconciliation",
            "--------------",
            f"Status: {summary.reconciliation_status}",
            "",
            "Failure Diagnostics",
            "--------------------",
            f"Failure stage: {self._format_value(summary.failure_stage)}",
            f"Failure type: {self._format_value(summary.failure_type)}",
            f"Retry count: {self._format_value(summary.retry_count)}",
            f"Total attempts: {self._format_value(summary.total_attempts)}",
            f"Error message: {self._format_value(summary.error_message)}",
        ]

        return "\n".join(lines)

    @staticmethod
    def _format_value(
        value: object,
    ) -> str:
        """
        Converts optional values into notification-safe text.
        """

        if value is None:
            return "N/A"

        return str(value)