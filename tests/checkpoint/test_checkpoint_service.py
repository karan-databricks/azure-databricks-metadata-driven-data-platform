# tests/checkpoint/test_checkpoint_service.py

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from enterprise_api_ingestion_platform.checkpoint.checkpoint_model import (
    Checkpoint,
)
from enterprise_api_ingestion_platform.checkpoint.checkpoint_service import (
    CheckpointService,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


def create_metadata(**overrides: object) -> ApiMetadata:
    values: dict[str, object] = {
        "api_id": "orders-api",
        "api_name": "orders",
        "endpoint": "https://example.com/orders",
        "load_type": "INCREMENTAL",
    }
    values.update(overrides)
    return ApiMetadata(**values)


def test_get_checkpoint_uses_api_id() -> None:
    repository = MagicMock()
    checkpoint = Checkpoint(
        api_id="orders-api",
        state_type="CURSOR",
        state_value="cursor-123",
        recorded_at=datetime(2026, 9, 7, 10, 0),
        run_id="run-123",
    )
    repository.get_last_success_checkpoint.return_value = checkpoint

    service = CheckpointService(repository)
    metadata = create_metadata()

    result = service.get_checkpoint(metadata)

    assert result == checkpoint
    repository.get_last_success_checkpoint.assert_called_once_with(
        "orders-api",
    )


def test_get_checkpoint_returns_none_without_api_id() -> None:
    repository = MagicMock()
    service = CheckpointService(repository)

    metadata = create_metadata(api_id=None)

    result = service.get_checkpoint(metadata)

    assert result is None
    repository.get_last_success_checkpoint.assert_not_called()


def test_full_load_returns_no_watermark() -> None:
    repository = MagicMock()
    service = CheckpointService(repository)

    metadata = create_metadata(load_type="FULL")

    result = service.get_effective_watermark(metadata)

    assert result is None
    repository.get_last_success_checkpoint.assert_not_called()


def test_get_effective_watermark_uses_checkpoint() -> None:
    repository = MagicMock()
    checkpoint_time = datetime(2026, 9, 7, 10, 0)

    repository.get_last_success_checkpoint.return_value = Checkpoint(
        api_id="orders-api",
        state_type="WATERMARK",
        state_value=checkpoint_time.isoformat(),
        recorded_at=datetime(2026, 9, 7, 11, 0),
        run_id="run-123",
    )

    service = CheckpointService(repository)
    metadata = create_metadata()

    result = service.get_effective_watermark(metadata)

    assert result == checkpoint_time


def test_get_effective_watermark_applies_overlap() -> None:
    repository = MagicMock()
    checkpoint_time = datetime(2026, 9, 7, 10, 0)

    repository.get_last_success_checkpoint.return_value = Checkpoint(
        api_id="orders-api",
        state_type="WATERMARK",
        state_value=checkpoint_time.isoformat(),
        recorded_at=datetime(2026, 9, 7, 11, 0),
        run_id="run-123",
    )

    service = CheckpointService(repository)
    metadata = create_metadata(overlap_minutes=15)

    result = service.get_effective_watermark(metadata)

    assert result == checkpoint_time - timedelta(minutes=15)


def test_get_effective_watermark_uses_initial_load_value() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = None

    service = CheckpointService(repository)

    metadata = create_metadata(
        initial_load_value="2026-09-01T12:30:00+00:00",
    )

    result = service.get_effective_watermark(metadata)

    assert result == datetime.fromisoformat(
        "2026-09-01T12:30:00+00:00",
    )


def test_get_effective_watermark_applies_overlap_to_initial_value() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = None

    service = CheckpointService(repository)

    metadata = create_metadata(
        initial_load_value="2026-09-01T12:30:00+00:00",
        overlap_minutes=30,
    )

    result = service.get_effective_watermark(metadata)

    assert result == datetime.fromisoformat(
        "2026-09-01T12:00:00+00:00",
    )


def test_get_effective_watermark_returns_none_without_initial_value() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = None

    service = CheckpointService(repository)
    metadata = create_metadata()

    result = service.get_effective_watermark(metadata)

    assert result is None


def test_get_effective_cursor_returns_persisted_cursor() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = Checkpoint(
        api_id="test-api",
        state_type="CURSOR",
        state_value="cursor-123",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    service = CheckpointService(repository)
    metadata = create_metadata(
        api_id="test-api",
        api_name="test-api",
    )

    assert service.get_effective_cursor(metadata) == "cursor-123"


def test_get_effective_cursor_returns_none_when_checkpoint_missing() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = None

    service = CheckpointService(repository)
    metadata = create_metadata(
        api_id="test-api",
        api_name="test-api",
    )

    assert service.get_effective_cursor(metadata) is None


def test_get_effective_cursor_returns_none_for_full_load() -> None:
    repository = MagicMock()

    service = CheckpointService(repository)
    metadata = create_metadata(
        api_id="test-api",
        api_name="test-api",
        load_type="FULL",
    )

    assert service.get_effective_cursor(metadata) is None
    repository.get_last_success_checkpoint.assert_not_called()


def test_get_effective_cursor_returns_none_for_watermark_checkpoint() -> None:
    repository = MagicMock()
    repository.get_last_success_checkpoint.return_value = Checkpoint(
        api_id="test-api",
        state_type="WATERMARK",
        state_value="2026-09-07T12:00:00Z",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    service = CheckpointService(repository)
    metadata = create_metadata(
        api_id="test-api",
        api_name="test-api",
    )

    assert service.get_effective_cursor(metadata) is None


def test_save_checkpoint_persists_cursor_state() -> None:
    checkpoint_repository = MagicMock()

    service = CheckpointService(
        checkpoint_repository=checkpoint_repository,
    )

    metadata = ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        pagination_type="CURSOR",
        load_type="INCREMENTAL",
    )

    recorded_at = datetime(2026, 9, 7, 10, 1)

    service.save_checkpoint(
        metadata=metadata,
        checkpoint_state="cursor-next",
        run_id="run-123",
        recorded_at=recorded_at,
    )

    checkpoint_repository.save_checkpoint.assert_called_once_with(
        api_id="orders-api",
        state_type="CURSOR",
        state_value="cursor-next",
        recorded_at=recorded_at,
        run_id="run-123",
    )