# tests/checkpoint/test_checkpoint_model.py

from datetime import datetime, timezone

from enterprise_api_ingestion_platform.checkpoint.checkpoint_model import (
    Checkpoint,
)


def test_cursor_returns_cursor_value():
    checkpoint = Checkpoint(
        api_id="test-api",
        state_type="CURSOR",
        state_value="cursor-123",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    assert checkpoint.cursor == "cursor-123"


def test_watermark_does_not_return_cursor():
    checkpoint = Checkpoint(
        api_id="test-api",
        state_type="WATERMARK",
        state_value="2026-09-07T12:00:00Z",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    assert checkpoint.cursor is None


def test_watermark_returns_parsed_datetime():
    checkpoint = Checkpoint(
        api_id="test-api",
        state_type="WATERMARK",
        state_value="2026-09-07T12:00:00Z",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    assert checkpoint.watermark == datetime(
        2026,
        9,
        7,
        12,
        0,
        tzinfo=timezone.utc,
    )


def test_cursor_does_not_return_watermark():
    checkpoint = Checkpoint(
        api_id="test-api",
        state_type="CURSOR",
        state_value="cursor-123",
        recorded_at=datetime.now(timezone.utc),
        run_id="run-123",
    )

    assert checkpoint.watermark is None