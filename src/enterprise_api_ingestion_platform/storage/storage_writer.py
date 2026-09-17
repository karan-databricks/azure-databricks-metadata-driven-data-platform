from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable

from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class StorageWriter(ABC):
    """
    Contract for writing landing files to a storage platform.
    """

    @abstractmethod
    def write(
        self,
        metadata: ApiMetadata,
        payload: Any,
    ) -> str:
        """
        Writes a complete payload to storage.

        Returns
        -------
        str
            Fully qualified path of the written file.
        """
        raise NotImplementedError

    @abstractmethod
    def write_stream(
        self,
        metadata: ApiMetadata,
        chunks: Iterable[bytes],
    ) -> str:
        """
        Writes a streamed payload to storage.

        Returns
        -------
        str
            Fully qualified path of the written file.
        """
        raise NotImplementedError
        
    @abstractmethod
    def get_file_size(
        self,
        file_path: str,
    ) -> int:
        """
        Returns the size of a previously written file in bytes.
        """
        raise NotImplementedError