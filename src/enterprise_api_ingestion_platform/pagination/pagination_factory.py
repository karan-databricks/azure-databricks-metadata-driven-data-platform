# src/enterprise_api_ingestion_platform/pagination/pagination_factory.py

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
from enterprise_api_ingestion_platform.pagination.pagination_strategy import (
    PaginationStrategy,
)


class PaginationFactory:
    """
    Factory responsible for creating the appropriate pagination strategy
    based on API metadata.
    """

    @staticmethod
    def create(metadata: ApiMetadata) -> PaginationStrategy:
        """
        Creates a pagination strategy from API metadata.
        """
        pagination_type = (metadata.pagination_type or "NONE").upper()

        if pagination_type == "NONE":
            return NonePagination()

        if pagination_type == "OFFSET":
            if metadata.page_size is None:
                raise ValueError(
                    "page_size must be configured for OFFSET pagination."
                )

            return OffsetPagination(
                page_size=metadata.page_size,
            )

        if pagination_type == "CURSOR":
            return CursorPagination(
                cursor_field=metadata.cursor_field,
                cursor_parameter=metadata.cursor_parameter,
            )

        raise UnsupportedPaginationTypeException(
            f"Unsupported pagination type: {pagination_type}"
        )