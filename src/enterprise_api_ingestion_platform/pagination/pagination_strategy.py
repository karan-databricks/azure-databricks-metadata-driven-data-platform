from abc import ABC, abstractmethod
from typing import Any


class PaginationStrategy(ABC):
    """
    Abstract base class for API pagination strategies.
    """

    @abstractmethod
    def get_initial_params(
        self,
        query_params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Returns the query parameters for the first API request.
        """

    @abstractmethod
    def has_next_page(
        self,
        response_json: Any,
    ) -> bool:
        """
        Returns True if another page should be requested.
        """

    @abstractmethod
    def get_next_params(
        self,
        current_params: dict[str, Any],
        response_json: Any,
    ) -> dict[str, Any]:
        """
        Returns the query parameters for the next API request.
        """