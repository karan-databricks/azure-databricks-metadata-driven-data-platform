# src/enterprise_api_ingestion_platform/models/landing_result.py

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LandingResult:
    """
    Represents the result of a successful landing write.
    """

    landing_file: str
    checkpoint_state: str | None = None