# tests/metadata/test_metadata_reader.py

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from enterprise_api_ingestion_platform.common.exceptions import (
    DuplicateMetadataException,
    MetadataNotFoundException,
)
from enterprise_api_ingestion_platform.metadata.metadata_reader import (
    MetadataReader,
)


def _build_row(**overrides):
    values = {
        "api_id": "test-api-id",
        "api_name": "test_api",
        "endpoint": "https://example.com/api",
        "http_method": "GET",
        "auth_type": "NONE",
        "secret_scope": None,
        "secret_key": None,
        "headers_json": '{"Accept": "application/json"}',
        "query_parameters": '{"limit": 100}',
        "bronze_catalog": "workspace",
        "bronze_schema": "bronze",
        "bronze_table": "test_api",
        "record_path": None,
        "pagination_type": "NONE",
        "page_size": None,
        "cursor_field": None,
        "cursor_parameter": None,
        "load_type": "FULL",
        "incremental_column": None,
        "incremental_parameter": None,
        "incremental_format": None,
        "overlap_minutes": None,
        "initial_load_value": None,
        "enabled": True,
        "created_by": "test",
        "created_ts": None,
        "updated_ts": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_get_metadata_returns_api_metadata():
    spark = MagicMock()
    dataframe = MagicMock()

    dataframe.filter.return_value = dataframe
    dataframe.collect.return_value = [_build_row()]
    spark.table.return_value = dataframe

    reader = MetadataReader(spark)

    metadata = reader.get_metadata("test_api")

    assert metadata.api_id == "test-api-id"
    assert metadata.api_name == "test_api"
    assert metadata.endpoint == "https://example.com/api"
    assert metadata.headers_json == {"Accept": "application/json"}
    assert metadata.query_parameters == {"limit": 100}
    assert metadata.cursor_field == "next_cursor"
    assert metadata.cursor_parameter == "cursor"


def test_get_metadata_raises_when_api_is_missing():
    spark = MagicMock()
    dataframe = MagicMock()

    dataframe.filter.return_value = dataframe
    dataframe.collect.return_value = []
    spark.table.return_value = dataframe

    reader = MetadataReader(spark)

    with pytest.raises(MetadataNotFoundException):
        reader.get_metadata("missing_api")


def test_get_metadata_raises_when_api_is_duplicated():
    spark = MagicMock()
    dataframe = MagicMock()

    dataframe.filter.return_value = dataframe
    dataframe.collect.return_value = [
        _build_row(),
        _build_row(api_id="another-api-id"),
    ]
    spark.table.return_value = dataframe

    reader = MetadataReader(spark)

    with pytest.raises(DuplicateMetadataException):
        reader.get_metadata("test_api")


def test_optional_columns_use_defaults_when_absent():
    spark = MagicMock()
    reader = MetadataReader(spark)

    row = SimpleNamespace(
        api_id="test-api-id",
        api_name="test_api",
        endpoint="https://example.com/api",
        http_method="GET",
        auth_type="NONE",
        secret_scope=None,
        secret_key=None,
        headers_json=None,
        query_parameters=None,
        bronze_catalog="workspace",
        bronze_schema="bronze",
        bronze_table="test_api",
        record_path=None,
        pagination_type="NONE",
        page_size=None,
        load_type="FULL",
        incremental_column=None,
        enabled=True,
    )

    metadata = reader._to_api_metadata(row)

    assert metadata.auth_header_name is None
    assert metadata.auth_header_prefix is None
    assert metadata.landing_container == "landing"
    assert metadata.landing_path == ""
    assert metadata.download_mode == "BUFFER"
    assert metadata.schedule is None
    assert metadata.execution_group is None


def test_json_metadata_defaults_to_empty_dict():
    spark = MagicMock()
    reader = MetadataReader(spark)

    row = _build_row(
        headers_json=None,
        query_parameters=None,
    )

    metadata = reader._to_api_metadata(row)

    assert metadata.headers_json == {}
    assert metadata.query_parameters == {}


def test_invalid_json_metadata_raises():
    spark = MagicMock()
    reader = MetadataReader(spark)

    row = _build_row(headers_json="{invalid-json")

    with pytest.raises(ValueError):
        reader._to_api_metadata(row)