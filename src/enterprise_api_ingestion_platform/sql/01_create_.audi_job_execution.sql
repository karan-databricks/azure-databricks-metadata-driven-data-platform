CREATE TABLE IF NOT EXISTS workspace.audit.job_execution
(
    run_id STRING,

    api_name STRING,

    job_name STRING,

    start_time TIMESTAMP,

    end_time TIMESTAMP,

    status STRING,

    records_read INT,

    records_written INT,

    pages_read INT,
	
	schema_version INT,

    duration_seconds DOUBLE,

    landing_file STRING,

    landing_file_size_bytes BIGINT,

    failure_stage STRING,

    failure_type STRING,

    http_status INT,

    retry_count INT,

    total_attempts INT,

    error_message STRING
)
USING DELTA;