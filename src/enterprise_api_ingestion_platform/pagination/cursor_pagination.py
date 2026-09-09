# src/enterprise_api_ingestion_platform/pagination/cursor_pagination.py

from typing import Any

from enterprise_api_ingestion_platform.pagination.pagination_strategy import (
    PaginationStrategy,
)


class CursorPagination(PaginationStrategy):
    """
    Pagination strategy for APIs that use cursor-based pagination.
    """

    def __init__(
        self,
        cursor_field: str = "next_cursor",
        cursor_parameter: str = "cursor",
    ) -> None:
        self.cursor_field = cursor_field
        self.cursor_parameter = cursor_parameter

    def get_initial_params(
        self,
        query_params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Returns the initial query parameters.
        """
        return query_params.copy() if query_params else {}

    def has_next_page(
        self,
        response_json: Any,
    ) -> bool:
        """
        Returns True when a next cursor exists.
        """
        if not isinstance(response_json, dict):
            return False

        cursor = response_json.get(self.cursor_field)

        return cursor is not None and cursor != ""

    def get_next_params(
        self,
        current_params: dict[str, Any],
        response_json: Any,
    ) -> dict[str, Any]:
        """
        Returns the query parameters for the next request.
        """
        params = current_params.copy()

        cursor = response_json.get(self.cursor_field)

        if cursor is not None:
            params[self.cursor_parameter] = cursor

        return params