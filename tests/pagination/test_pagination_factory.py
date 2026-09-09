# tests/pagination/test_pagination_factory.py

import pytest

from enterprise_api_ingestion_platform.common.exceptions import (
    UnsupportedPaginationTypeException,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.pagination.cursor_pagination import (
    CursorPagination,
)
from enterprise_api_ingestion_platform.pagination.none_pagination import (
    NonePagination,
)
from enterprise_api_ingestion_platform.pagination.offset_pagination import (
    OffsetPagination,
)
from enterprise_api_ingestion_platform.pagination.pagination_factory import (
    PaginationFactory,
)


def create_metadata(**overrides: object) -> ApiMetadata:
    values: dict[str, object] = {
        "api_id": "test-api",
        "api_name": "test-api",
        "endpoint": "https://example.com",
        "pagination_type": "NONE",
    }
    values.update(overrides)
    return ApiMetadata(**values)


def test_create_none_pagination() -> None:
    metadata = create_metadata(
        pagination_type="NONE",
    )

    result = PaginationFactory.create(metadata)

    assert isinstance(result, NonePagination)


def test_create_offset_pagination() -> None:
    metadata = create_metadata(
        pagination_type="OFFSET",
        page_size=100,
    )

    result = PaginationFactory.create(metadata)

    assert isinstance(result, OffsetPagination)


def test_create_offset_pagination_requires_page_size() -> None:
    metadata = create_metadata(
        pagination_type="OFFSET",
    )

    with pytest.raises(
        ValueError,
        match="page_size must be configured",
    ):
        PaginationFactory.create(metadata)


def test_create_cursor_pagination_with_defaults() -> None:
    metadata = create_metadata(
        pagination_type="CURSOR",
    )

    result = PaginationFactory.create(metadata)

    assert isinstance(result, CursorPagination)
    assert result.cursor_field == "next_cursor"
    assert result.cursor_parameter == "cursor"


def test_create_cursor_pagination_with_custom_configuration() -> None:
    metadata = create_metadata(
        pagination_type="CURSOR",
        cursor_field="nextToken",
        cursor_parameter="pageToken",
    )

    result = PaginationFactory.create(metadata)

    assert isinstance(result, CursorPagination)
    assert result.cursor_field == "nextToken"
    assert result.cursor_parameter == "pageToken"


def test_create_rejects_unsupported_pagination_type() -> None:
    metadata = create_metadata(
        pagination_type="UNKNOWN",
    )

    with pytest.raises(
        UnsupportedPaginationTypeException,
        match="Unsupported pagination type: UNKNOWN",
    ):
        PaginationFactory.create(metadata)


def test_missing_pagination_type_defaults_to_none() -> None:
    metadata = create_metadata(
        pagination_type=None,
    )

    result = PaginationFactory.create(metadata)

    assert isinstance(result, NonePagination)