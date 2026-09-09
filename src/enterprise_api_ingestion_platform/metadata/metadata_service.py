# src/enterprise_api_ingestion_platform/metadata/metadata_service.py

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from enterprise_api_ingestion_platform.config.environment import (
    environment,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class MetadataService:
    """
    Service responsible for retrieving API metadata from
    the Delta metadata table.
    """

    @property
    def metadata_table(self) -> str:
        """
        Returns the fully qualified metadata table name.
        """

        return (
            f"{environment.catalog}."
            f"{environment.metadata_schema}."
            "api_metadata"
        )

    def get_metadata(
        self,
        api_name: str,
    ) -> ApiMetadata:
        """
        Returns metadata for a single enabled API.

        Raises
        ------
        ValueError
            If the API does not exist or is disabled.
        """

        metadata = self._load_from_delta(api_name)

        if metadata is None:
            raise ValueError(
                f"No metadata found for API '{api_name}'."
            )

        return metadata

    def get_enabled_metadata(
        self,
    ) -> list[ApiMetadata]:
        """
        Returns all enabled APIs.
        """

        spark = SparkSession.getActiveSession()

        if spark is None:
            return []

        rows = (
            spark.table(self.metadata_table)
            .filter(col("enabled"))
            .orderBy(col("api_name"))
            .collect()
        )

        return [
            self._row_to_metadata(row)
            for row in rows
        ]

    def get_enabled_metadata_by_schedule(
        self,
        schedule: str,
        execution_group: str | None = None,
    ) -> list[ApiMetadata]:
        """
        Returns enabled APIs matching the specified schedule.

        Parameters
        ----------
        schedule
            Schedule identifier.

        execution_group
            Optional execution group filter.
        """

        spark = SparkSession.getActiveSession()

        if spark is None:
            return []

        dataframe = (
            spark.table(self.metadata_table)
            .filter(col("enabled"))
            .filter(col("schedule") == schedule)
        )

        if execution_group:
            dataframe = dataframe.filter(
                col("execution_group") == execution_group
            )

        rows = (
            dataframe
            .orderBy(col("api_name"))
            .collect()
        )

        return [
            self._row_to_metadata(row)
            for row in rows
        ]

    def _load_from_delta(
        self,
        api_name: str,
    ) -> ApiMetadata | None:
        """
        Loads metadata for a single enabled API.
        """

        spark = SparkSession.getActiveSession()

        if spark is None:
            return None

        rows = (
            spark.table(self.metadata_table)
            .filter(
                (col("api_name") == api_name)
                & col("enabled")
            )
            .limit(1)
            .collect()
        )

        if not rows:
            return None

        return self._row_to_metadata(rows[0])

    @staticmethod
    def _parse_json_object(
        value: Any,
    ) -> dict[str, Any] | None:
        """
        Converts a JSON string or dictionary into a dictionary.
        """

        if value is None:
            return None

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            value = value.strip()

            if not value:
                return None

            parsed = json.loads(value)

            if not isinstance(parsed, dict):
                raise ValueError(
                    "Expected JSON object metadata."
                )

            return parsed

        raise TypeError(
            f"Expected JSON object or string, got {type(value).__name__}."
        )

    @staticmethod
    def _row_to_metadata(
        row,
    ) -> ApiMetadata:
        """
        Converts a Spark Row into an ApiMetadata object.
        """

        return ApiMetadata(
            api_id=getattr(row, "api_id", None),
            api_name=row.api_name,
            endpoint=row.endpoint,
            http_method=row.http_method,
            auth_type=row.auth_type,
            secret_scope=row.secret_scope,
            secret_key=row.secret_key,
            auth_header_name=getattr(
                row,
                "auth_header_name",
                None,
            ),
            auth_header_prefix=getattr(
                row,
                "auth_header_prefix",
                None,
            ),
            headers_json=MetadataService._parse_json_object(
                getattr(row, "headers_json", None),
            ),
            query_parameters=MetadataService._parse_json_object(
                getattr(row, "query_parameters", None),
            ),
            bronze_catalog=getattr(
                row,
                "bronze_catalog",
                None,
            ),
            bronze_schema=getattr(
                row,
                "bronze_schema",
                None,
            ),
            bronze_table=getattr(
                row,
                "bronze_table",
                None,
            ),
            record_path=getattr(
                row,
                "record_path",
                None,
            ),
            landing_container=getattr(
                row,
                "landing_container",
                None,
            ),
            landing_path=getattr(
                row,
                "landing_path",
                None,
            ),
            pagination_type=row.pagination_type,
            page_size=row.page_size,
            load_type=row.load_type,
            incremental_column=row.incremental_column,
            incremental_parameter=getattr(
                row,
                "incremental_parameter",
                None,
            ),
            incremental_format=getattr(
                row,
                "incremental_format",
                None,
            ),
            overlap_minutes=getattr(
                row,
                "overlap_minutes",
                None,
            ),
            initial_load_value=getattr(
                row,
                "initial_load_value",
                None,
            ),
            download_mode=getattr(
                row,
                "download_mode",
                "BUFFER",
            ) or "BUFFER",
            schedule=getattr(
                row,
                "schedule",
                None,
            ),
            execution_group=getattr(
                row,
                "execution_group",
                None,
            ),
            enabled=row.enabled,
            created_by=getattr(
                row,
                "created_by",
                None,
            ),
            created_ts=getattr(
                row,
                "created_ts",
                None,
            ),
            updated_ts=getattr(
                row,
                "updated_ts",
                None,
            ),
        )