# src/enterprise_api_ingestion_platform/checkpoint/checkpoint_service.py

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from enterprise_api_ingestion_platform.checkpoint.checkpoint_model import (
    Checkpoint,
)
from enterprise_api_ingestion_platform.checkpoint.checkpoint_repository import (
    CheckpointRepository,
)
from enterprise_api_ingestion_platform.models.api_metadata import (
    ApiMetadata,
)


class CheckpointService:
    """
    Encapsulates checkpoint and watermark business logic.
    """

    def __init__(
        self,
        checkpoint_repository: CheckpointRepository,
    ) -> None:
        self._checkpoint_repository = checkpoint_repository

    def get_checkpoint(
        self,
        metadata: ApiMetadata,
    ) -> Optional[Checkpoint]:
        """
        Returns the latest successful checkpoint for the API.
        """
        if not metadata.api_id:
            return None

        return self._checkpoint_repository.get_last_success_checkpoint(
            metadata.api_id,
        )

    def get_effective_watermark(
        self,
        metadata: ApiMetadata,
    ) -> Optional[datetime]:
        """
        Determines the watermark to use for the next extraction.
        """
        if metadata.load_type.upper() == "FULL":
            return None

        checkpoint = self.get_checkpoint(metadata)

        if checkpoint is not None:
            watermark = checkpoint.watermark
        else:
            watermark = self._parse_initial_load_value(
                metadata.initial_load_value,
            )

        if watermark is None:
            return None

        overlap_minutes = metadata.overlap_minutes or 0

        if overlap_minutes > 0:
            watermark = watermark - timedelta(
                minutes=overlap_minutes,
            )

        return watermark

    def get_effective_cursor(
        self,
        metadata: ApiMetadata,
    ) -> Optional[str]:
        """
        Determines the cursor to use for the next extraction.
        """
        if metadata.load_type.upper() == "FULL":
            return None

        checkpoint = self.get_checkpoint(metadata)

        if checkpoint is None:
            return None

        return checkpoint.cursor

    @staticmethod
    def _parse_initial_load_value(
        value: str | None,
    ) -> Optional[datetime]:
        """
        Parse the configured initial watermark value.
        """
        if value is None:
            return None

        return datetime.fromisoformat(
            value.replace("Z", "+00:00"),
        )

    def save_checkpoint(
        self,
        metadata: ApiMetadata,
        checkpoint_state: str | None,
        run_id: str,
        recorded_at: datetime,
    ) -> None:
        """
        Persists a successful checkpoint when the API produces checkpoint state.
        """
        if not metadata.api_id:
            return

        if checkpoint_state is None:
            return

        if (metadata.pagination_type or "NONE").upper() == "CURSOR":
            self._checkpoint_repository.save_checkpoint(
                api_id=metadata.api_id,
                state_type="CURSOR",
                state_value=checkpoint_state,
                recorded_at=recorded_at,
                run_id=run_id,
            )