from pathlib import PurePosixPath

from enterprise_api_ingestion_platform.framework.constants import (
    GRAPH_USERS_PATH,
    LANDING_VOLUME,
)


class PathBuilder:
    """Builds storage paths used throughout the platform."""

    @staticmethod
    def graph_users() -> str:
        return str(
            PurePosixPath(LANDING_VOLUME) / GRAPH_USERS_PATH
        )