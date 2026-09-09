-- src/enterprise_api_ingestion_platform/sql/01_create_metadata_tables.sql

-- Metadata Table
-- One record = One API Configuration
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS workspace.config.api_metadata
(
    api_id STRING NOT NULL,

    api_name STRING NOT NULL,

    endpoint STRING NOT NULL,

    http_method STRING NOT NULL,

    auth_type STRING NOT NULL,

    secret_scope STRING,

    secret_key STRING,

    headers_json STRING,

    query_parameters STRING,

    bronze_catalog STRING NOT NULL,

    bronze_schema STRING NOT NULL,

    bronze_table STRING NOT NULL,

    record_path STRING,

    pagination_type STRING,

    page_size INT,

    load_type STRING,

    incremental_column STRING,

    enabled BOOLEAN,

    created_by STRING,

    created_ts TIMESTAMP,

    updated_ts TIMESTAMP
)
USING DELTA;