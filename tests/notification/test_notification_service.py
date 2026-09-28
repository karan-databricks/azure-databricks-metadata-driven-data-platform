from enterprise_api_ingestion_platform.notification.notification_service import (
    NotificationService,
    NotificationSummary,
)


def test_build_subject() -> None:
    service = NotificationService()

    summary = NotificationSummary(
        job_run_id="12345",
        job_name="generic_api_ingestion_job",
        overall_status="SUCCESS",
        api_records_read=100,
        pages_read=5,
        landing_file="/Volumes/workspace/landing/file.json",
        landing_file_size_bytes=2048,
        ingestion_status="SUCCESS",
        bronze_status="SUCCESS",
        silver_status="SUCCESS",
        gold_status="SUCCESS",
        dq_status="PASSED",
        reconciliation_status="PASSED",
        failure_stage=None,
        failure_type=None,
        retry_count=0,
        total_attempts=1,
        error_message=None,
    )

    assert (
        service.build_subject(summary)
        == "API Ingestion Run Summary - SUCCESS - 12345"
    )


def test_build_body_contains_operational_summary() -> None:
    service = NotificationService()

    summary = NotificationSummary(
        job_run_id="12345",
        job_name="generic_api_ingestion_job",
        overall_status="SUCCESS",
        api_records_read=100,
        pages_read=5,
        landing_file="/Volumes/workspace/landing/file.json",
        landing_file_size_bytes=2048,
        ingestion_status="SUCCESS",
        bronze_status="SUCCESS",
        silver_status="SUCCESS",
        gold_status="SUCCESS",
        dq_status="PASSED",
        reconciliation_status="PASSED",
        failure_stage=None,
        failure_type=None,
        retry_count=0,
        total_attempts=1,
        error_message=None,
    )

    body = service.build_body(summary)

    assert "Job: generic_api_ingestion_job" in body
    assert "Run ID: 12345" in body
    assert "API records read: 100" in body
    assert "Pages read: 5" in body
    assert "Landing file size (bytes): 2048" in body
    assert "Bronze: SUCCESS" in body
    assert "Silver: SUCCESS" in body
    assert "Gold: SUCCESS" in body
    assert "DQ status: PASSED" in body
    assert "Status: PASSED" in body
    assert "Failure stage: N/A" in body
    assert "Retry count: 0" in body


def test_build_body_formats_missing_values_as_na() -> None:
    service = NotificationService()

    summary = NotificationSummary(
        job_run_id="67890",
        job_name="generic_api_ingestion_job",
        overall_status="FAILED",
        api_records_read=None,
        pages_read=None,
        landing_file=None,
        landing_file_size_bytes=None,
        ingestion_status="FAILED",
        bronze_status="SKIPPED",
        silver_status="SKIPPED",
        gold_status="SKIPPED",
        dq_status="NOT_EVALUATED",
        reconciliation_status="NOT_EVALUATED",
        failure_stage="LANDING",
        failure_type="HTTP_ERROR",
        retry_count=None,
        total_attempts=None,
        error_message="API request failed.",
    )

    body = service.build_body(summary)

    assert "API records read: N/A" in body
    assert "Pages read: N/A" in body
    assert "Landing file: N/A" in body
    assert "Landing file size (bytes): N/A" in body
    assert "Retry count: N/A" in body
    assert "Total attempts: N/A" in body
    assert "Failure stage: LANDING" in body
    assert "Failure type: HTTP_ERROR" in body
    assert "Error message: API request failed." in body