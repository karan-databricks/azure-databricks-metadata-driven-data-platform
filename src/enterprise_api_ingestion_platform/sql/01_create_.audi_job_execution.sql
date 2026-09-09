CREATE TABLE IF NOT EXISTS workspace.audit.job_execution
(
    run_id STRING,

    api_name STRING,

    job_name STRING,

    start_time TIMESTAMP,

    end_time TIMESTAMP,

    status STRING,

    records_read BIGINT,

    records_written BIGINT,

    duration_seconds DOUBLE,

    error_message STRING
)
USING DELTA;