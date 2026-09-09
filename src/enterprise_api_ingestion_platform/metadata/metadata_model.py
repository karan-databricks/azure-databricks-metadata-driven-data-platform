from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class ApiMetadata:
    api_id: str
    api_name: str
    endpoint: str
    http_method: str
    auth_type: str

    secret_scope: Optional[str]
    secret_key: Optional[str]

    headers_json: Dict[str, Any]
    query_parameters: Dict[str, Any]

    bronze_catalog: str
    bronze_schema: str
    bronze_table: str

    pagination_type: Optional[str]
    page_size: Optional[int]

    load_type: str
    incremental_column: Optional[str]

    enabled: bool

    created_by: Optional[str]
    created_ts: Optional[str]
    updated_ts: Optional[str]