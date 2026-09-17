from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LandingResult:
    """
    Represents the result of a successful landing operation.

    The result carries both the landing artifact information
    and the ingestion metrics required by the execution audit.
    """

    landing_file: str
    checkpoint_state: str | None = None
    records_read: int | None = None
    pages_read: int | None = None
    landing_file_size_bytes: int | None = None