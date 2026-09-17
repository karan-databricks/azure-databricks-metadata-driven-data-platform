# src/enterprise_api_ingestion_platform/landing/landing_writer.py

from __future__ import annotations

from typing import Iterable

from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.storage.storage_writer import StorageWriter


class LandingWriter:
    """
    Delegates landing-file operations to the configured storage writer.
    """

    def __init__(
        self,
        storage_writer: StorageWriter,
    ) -> None:
        self._storage_writer = storage_writer

    def write(
        self,
        metadata: ApiMetadata,
        payload: object,
    ) -> str:
        """
        Writes a complete payload to the configured landing storage.
        """

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
        Writes a streamed payload to the configured landing storage.
        """

        return self._storage_writer.write_stream(
            metadata=metadata,
            chunks=chunks,
        )

    def get_file_size(
        self,
        file_path: str,
    ) -> int:
        """
        Returns the size of a landed file in bytes.
        """

        return self._storage_writer.get_file_size(
            file_path=file_path,
        )