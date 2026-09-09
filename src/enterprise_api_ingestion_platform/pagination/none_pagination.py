from typing import Any

from enterprise_api_ingestion_platform.pagination.pagination_strategy import (
    PaginationStrategy,
)


class NonePagination(PaginationStrategy):
    """
    Pagination strategy for APIs that do not support pagination.
    """

    def get_initial_params(
        self,
        query_params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Returns the initial query parameters unchanged.
        """

        return query_params.copy() if query_params else {}

    def has_next_page(
        self,
        response_json: Any,
    ) -> bool:
        """
        No additional pages exist.
        """

        return False

    def get_next_params(
        self,
        current_params: dict[str, Any],
        response_json: Any,
    ) -> dict[str, Any]:
        """
        No next page exists.
        """

        return current_params