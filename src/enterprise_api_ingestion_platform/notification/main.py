# src/enterprise_api_ingestion_platform/notification/main.py

import argparse

from enterprise_api_ingestion_platform.logging.logger import get_logger
from enterprise_api_ingestion_platform.notification.notification_service import (
    NotificationService,
    NotificationSummary,
)


def parse_arguments() -> argparse.Namespace:
    """
    Parse notification command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description="Enterprise API Ingestion Notification",
    )

    parser.add_argument(
        "--job_run_id",
        required=True,
        help="Databricks Job Run ID.",
    )

    parser.add_argument(
        "--job_name",
        required=True,
        help="Databricks Job name.",
    )

    parser.add_argument(
        "--overall_status",
        required=True,
        help="Overall job status.",
    )

    return parser.parse_args()


def main() -> None:
    """
    Notification entry point.

    This initial implementation validates the notification
    composition path. Delivery will be added separately.
    """

    args = parse_arguments()

    logger = get_logger(__name__)

    summary = NotificationSummary(
        job_run_id=args.job_run_id,
        job_name=args.job_name,
        overall_status=args.overall_status,
        api_records_read=None,
        pages_read=None,
        landing_file=None,
        landing_file_size_bytes=None,
        ingestion_status="UNKNOWN",
        bronze_status="UNKNOWN",
        silver_status="UNKNOWN",
        gold_status="UNKNOWN",
        dq_status="UNKNOWN",
        reconciliation_status="UNKNOWN",
        failure_stage=None,
        failure_type=None,
        retry_count=None,
        total_attempts=None,
        error_message=None,
    )

    service = NotificationService()

    subject = service.build_subject(summary)
    body = service.build_body(summary)

    logger.info("Notification subject: %s", subject)
    logger.info("Notification body:\n%s", body)


if __name__ == "__main__":
    main()