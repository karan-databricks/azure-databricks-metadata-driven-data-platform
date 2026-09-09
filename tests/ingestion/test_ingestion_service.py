# tests/ingestion/test_ingestion_service.py

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from enterprise_api_ingestion_platform.checkpoint.checkpoint_service import (
    CheckpointService,
)
from enterprise_api_ingestion_platform.ingestion.ingestion_service import (
    IngestionService,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.models.landing_result import LandingResult


def create_metadata() -> ApiMetadata:
    return ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        load_type="FULL",
    )


def test_run_successfully_completes_landing_and_audit() -> None:
    landing_service = MagicMock()
    landing_service.ingest.return_value = LandingResult(
        landing_file="/landing/orders.json",
    )

    audit_logger = MagicMock()
    checkpoint_service = MagicMock(spec=CheckpointService)
    checkpoint_service.get_effective_cursor.return_value = None

    service = IngestionService(
        landing_service=landing_service,
        audit_logger=audit_logger,
        checkpoint_service=checkpoint_service,
    )

    metadata = create_metadata()

    with patch(
        "enterprise_api_ingestion_platform.ingestion.ingestion_service.datetime"
    ) as datetime_mock:
        datetime_mock.now.side_effect = [
            datetime(2026, 9, 7, 10, 0),
            datetime(2026, 9, 7, 10, 0, 30),
            datetime(2026, 9, 7, 10, 1),
        ]

        service.run(metadata)

    landing_service.ingest.assert_called_once_with(
        metadata,
        initial_cursor=None,
    )

    audit_logger.log_execution.assert_called_once()

    audit_kwargs = audit_logger.log_execution.call_args.kwargs

    assert audit_kwargs["api_name"] == "orders"
    assert audit_kwargs["status"] == "SUCCESS"
    assert audit_kwargs["landing_file"] == "/landing/orders.json"
    assert audit_kwargs["error_message"] is None


def test_run_failure_logs_failed_audit_and_reraises() -> None:
    landing_service = MagicMock()
    landing_service.ingest.side_effect = RuntimeError(
        "Landing failed",
    )

    audit_logger = MagicMock()
    checkpoint_service = MagicMock(spec=CheckpointService)

    service = IngestionService(
        landing_service=landing_service,
        audit_logger=audit_logger,
        checkpoint_service=checkpoint_service,
    )

    metadata = create_metadata()

    with patch(
        "enterprise_api_ingestion_platform.ingestion.ingestion_service.datetime"
    ) as datetime_mock:
        datetime_mock.now.side_effect = [
            datetime(2026, 9, 7, 10, 0),
            datetime(2026, 9, 7, 10, 0, 30),
            datetime(2026, 9, 7, 10, 1),
        ]

        with pytest.raises(RuntimeError, match="Landing failed"):
            service.run(metadata)

    audit_logger.log_execution.assert_called_once()

    audit_kwargs = audit_logger.log_execution.call_args.kwargs

    assert audit_kwargs["api_name"] == "orders"
    assert audit_kwargs["status"] == "FAILED"
    assert audit_kwargs["landing_file"] is None
    assert audit_kwargs["error_message"] == "Landing failed"
    
    checkpoint_service.save_checkpoint.assert_not_called()


def test_run_uses_checkpoint_cursor_for_landing() -> None:
    landing_service = MagicMock()
    landing_service.ingest.return_value = LandingResult(
        landing_file="/landing/orders.json",
        checkpoint_state="cursor-next",
    )

    audit_logger = MagicMock()

    checkpoint_service = MagicMock(
        spec=CheckpointService,
    )
    checkpoint_service.get_effective_cursor.return_value = "cursor-previous"

    service = IngestionService(
        landing_service=landing_service,
        audit_logger=audit_logger,
        checkpoint_service=checkpoint_service,
    )

    metadata = ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        pagination_type="CURSOR",
        load_type="INCREMENTAL",
    )

    with patch(
        "enterprise_api_ingestion_platform.ingestion.ingestion_service.datetime"
    ) as datetime_mock:
        datetime_mock.now.side_effect = [
            datetime(2026, 9, 7, 10, 0),
            datetime(2026, 9, 7, 10, 0, 30),
            datetime(2026, 9, 7, 10, 1),
        ]

        service.run(metadata)

    checkpoint_service.get_effective_cursor.assert_called_once_with(
        metadata,
    )

    landing_service.ingest.assert_called_once_with(
        metadata,
        initial_cursor="cursor-previous",
    )
    
    checkpoint_service.save_checkpoint.assert_called_once_with(
        metadata=metadata,
        checkpoint_state="cursor-next",
        run_id=checkpoint_service.save_checkpoint.call_args.kwargs["run_id"],
        recorded_at=checkpoint_service.save_checkpoint.call_args.kwargs["recorded_at"],
    )