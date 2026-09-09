from __future__ import annotations

import logging
from typing import Any, Iterable

from enterprise_api_ingestion_platform.models.api_metadata import (
    ApiMetadata,
)
from enterprise_api_ingestion_platform.storage.storage_writer import (
    StorageWriter,
)

logger = logging.getLogger(__name__)


class LandingWriter:
    """
    Writes raw API responses to the Landing zone.
    """

    def __init__(
        self,
        storage_writer: StorageWriter,
    ) -> None:
        self._storage_writer = storage_writer

    def write(
        self,
        metadata: ApiMetadata,
        payload: Any,
    ) -> str:
        """
        Writes a buffered payload.
        """

        logger.info(
            "LandingWriter.write() invoked for api=%s",
            metadata.api_name,
        )

        return self._storage_writer.write(
            metadata=metadata,
            payload=payload,
        )

    def write_stream(
        self,
        metadata: ApiMetadata,
        chunks: Iterable[bytes],
    ) -> str:
        """
        Writes a streamed payload.
        """

        logger.info(
            "LandingWriter.write_stream() invoked for api=%s",
            metadata.api_name,
        )

        return self._storage_writer.write_stream(
            metadata=metadata,
            chunks=chunks,
        )