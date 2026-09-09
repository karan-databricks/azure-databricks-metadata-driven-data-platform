# src/enterprise_api_ingestion_platform/models/api_metadata.py

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(slots=True)
class ApiMetadata:
    """
    Metadata describing a single API ingestion configuration.
    """

    # Core API configuration
    api_id: Optional[str] = None
    api_name: str = ""
    endpoint: str = ""
    http_method: str = "GET"

    # Authentication
    auth_type: str = "NONE"
    secret_scope: Optional[str] = None
    secret_key: Optional[str] = None
    auth_header_name: Optional[str] = None
    auth_header_prefix: Optional[str] = None

    # Request configuration
    headers_json: Optional[dict[str, Any]] = None
    query_parameters: Optional[dict[str, Any]] = None

    # Landing / Bronze
    landing_container: str = "landing"
    landing_path: str = ""

    bronze_catalog: Optional[str] = None
    bronze_schema: Optional[str] = None
    bronze_table: Optional[str] = None
    record_path: Optional[str] = None

    # Pagination
    pagination_type: Optional[str] = None
    page_size: Optional[int] = None
    cursor_field: str = "next_cursor"
    cursor_parameter: str = "cursor"

    # Load configuration
    load_type: str = "FULL"
    incremental_column: Optional[str] = None
    incremental_parameter: Optional[str] = None
    incremental_format: Optional[str] = None
    overlap_minutes: Optional[int] = None
    initial_load_value: Optional[str] = None

    # Download configuration
    download_mode: str = "BUFFER"

    # Scheduling
    schedule: Optional[str] = None
    execution_group: Optional[str] = None

    # Metadata
    enabled: bool = True
    created_by: Optional[str] = None
    created_ts: Optional[datetime] = None
    updated_ts: Optional[datetime] = None