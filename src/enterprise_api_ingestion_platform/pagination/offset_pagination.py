from typing import Any

from enterprise_api_ingestion_platform.pagination.pagination_strategy import (
    PaginationStrategy,
)


class OffsetPagination(PaginationStrategy):
    """
    Pagination strategy for APIs using offset/limit pagination.
    """

    def __init__(
        self,
        page_size: int,
        offset_parameter: str = "offset",
        limit_parameter: str = "limit",
    ) -> None:
        if page_size <= 0:
            raise ValueError("page_size must be greater than zero.")

        self.page_size = page_size
        self.offset_parameter = offset_parameter
        self.limit_parameter = limit_parameter

    def get_initial_params(
        self,
        query_params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Returns the query parameters for the first request.
        """

        params = query_params.copy() if query_params else {}

        params[self.offset_parameter] = 0
        params[self.limit_parameter] = self.page_size

        return params

    def has_next_page(
        self,
        response_json: Any,
    ) -> bool:
        """
        Returns True when another page should be requested.
        """

        if not isinstance(response_json, list):
            return False

        return len(response_json) == self.page_size

    def get_next_params(
        self,
        current_params: dict[str, Any],
        response_json: Any,
    ) -> dict[str, Any]:
        """
        Returns the query parameters for the next page.
        """

        params = current_params.copy()

        current_offset = int(params.get(self.offset_parameter, 0))

        params[self.offset_parameter] = current_offset + self.page_size

        return params