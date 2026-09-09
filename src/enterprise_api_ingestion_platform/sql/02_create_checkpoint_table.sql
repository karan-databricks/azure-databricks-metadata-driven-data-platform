-- src/enterprise_api_ingestion_platform/sql/02_create_checkpoint_table.sql

-- API Extraction Checkpoint Table
-- One record = One successfully completed API extraction state
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS workspace.config.api_checkpoint
(
    api_id STRING NOT NULL,

    state_type STRING NOT NULL,

    state_value STRING NOT NULL,

    recorded_at TIMESTAMP NOT NULL,

    run_id STRING NOT NULL
)
USING DELTA;