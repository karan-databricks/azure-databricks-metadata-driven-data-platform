# src/enterprise_api_ingestion_platform/checkpoint/checkpoint_model.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """
    Represents the latest successfully persisted extraction state.

    The state value is intentionally stored as a string because API
    incremental state may be a cursor, timestamp, numeric token, or
    another source-defined value.
    """

    api_id: str
    state_type: str
    state_value: str
    recorded_at: datetime
    run_id: str

    @property
    def watermark(self) -> datetime | None:
        """
        Returns the checkpoint value as a UTC datetime when applicable.

        Returns
        -------
        datetime | None
            Parsed watermark for WATERMARK state, otherwise None.
        """

        if self.state_type.upper() != "WATERMARK":
            return None

        return datetime.fromisoformat(
            self.state_value.replace(
                "Z",
                "+00:00",
            )
        )

    @property
    def cursor(self) -> str | None:
        """
        Returns the checkpoint value when it represents cursor state.

        Returns
        -------
        str | None
            Cursor value for CURSOR state, otherwise None.
        """

        if self.state_type.upper() != "CURSOR":
            return None

        return self.state_value