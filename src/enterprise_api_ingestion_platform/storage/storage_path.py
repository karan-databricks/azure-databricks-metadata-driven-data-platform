from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StoragePath:
    """
    Represents the physical location of a landing file.
    """

    storage_account: str
    file_system: str
    directory: str
    file_name: str

    @property
    def abfss_uri(self) -> str:
        """
        Returns the fully qualified ABFSS URI.
        """
        return (
            f"abfss://{self.file_system}"
            f"@{self.storage_account}.dfs.core.windows.net/"
            f"{self.directory}/{self.file_name}"
        )