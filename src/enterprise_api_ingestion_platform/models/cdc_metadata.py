from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class CdcMetadata:
    """
    Metadata describing CDC configuration for a source.
    """

    cdc_config_id: str
    source_id: str
    cdc_mode: str
    operation_column: str
    sequence_column: str
    sequence_type: str
    delete_handling: str
    snapshot_key_column: str
    snapshot_strategy: str
    enabled: bool
    created_by: str
    created_ts: datetime
    updated_ts: datetime