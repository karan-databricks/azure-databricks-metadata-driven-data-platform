# tests/pagination/test_cursor_pagination.py

from enterprise_api_ingestion_platform.pagination.cursor_pagination import (
    CursorPagination,
)


def test_default_cursor_configuration() -> None:
    pagination = CursorPagination()

    assert pagination.cursor_field == "next_cursor"
    assert pagination.cursor_parameter == "cursor"


def test_get_initial_params_returns_copy() -> None:
    pagination = CursorPagination()

    query_params = {
        "limit": 100,
        "status": "active",
    }

    result = pagination.get_initial_params(query_params)

    assert result == query_params
    assert result is not query_params


def test_get_initial_params_returns_empty_dict_when_missing() -> None:
    pagination = CursorPagination()

    assert pagination.get_initial_params(None) == {}


def test_has_next_page_with_default_cursor_field() -> None:
    pagination = CursorPagination()

    assert pagination.has_next_page(
        {"data": [1, 2], "next_cursor": "cursor-123"},
    )


def test_has_next_page_returns_false_without_cursor() -> None:
    pagination = CursorPagination()

    assert not pagination.has_next_page(
        {"data": [1, 2]},
    )


def test_has_next_page_returns_false_for_empty_cursor() -> None:
    pagination = CursorPagination()

    assert not pagination.has_next_page(
        {"data": [1, 2], "next_cursor": ""},
    )


def test_has_next_page_returns_false_for_non_dict_response() -> None:
    pagination = CursorPagination()

    assert not pagination.has_next_page([1, 2, 3])


def test_custom_cursor_configuration() -> None:
    pagination = CursorPagination(
        cursor_field="nextToken",
        cursor_parameter="pageToken",
    )

    response = {
        "items": [1, 2],
        "nextToken": "token-456",
    }

    assert pagination.has_next_page(response)

    result = pagination.get_next_params(
        {"limit": 100},
        response,
    )

    assert result == {
        "limit": 100,
        "pageToken": "token-456",
    }


def test_get_next_params_preserves_existing_parameters() -> None:
    pagination = CursorPagination()

    current_params = {
        "limit": 50,
        "status": "active",
    }

    result = pagination.get_next_params(
        current_params,
        {"next_cursor": "cursor-789"},
    )

    assert result == {
        "limit": 50,
        "status": "active",
        "cursor": "cursor-789",
    }

    assert current_params == {
        "limit": 50,
        "status": "active",
    }


def test_get_next_params_without_cursor_does_not_modify_parameters() -> None:
    pagination = CursorPagination()

    current_params = {
        "limit": 50,
    }

    result = pagination.get_next_params(
        current_params,
        {"data": [1, 2]},
    )

    assert result == current_params
    assert result is not current_params