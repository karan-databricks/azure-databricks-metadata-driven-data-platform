from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class Environment:
    """
    Environment-specific configuration.
    """

    name: str
    catalog: str
    audit_schema: str
    metadata_schema: str
    secret_scope: str

    @classmethod
    def load(cls) -> "Environment":
        """
        Load environment-specific configuration.
        """

        environment = os.getenv(
            "ENVIRONMENT",
            "dev",
        ).lower()

        catalog_mapping = {
            "dev": "workspace",
            "qa": "adb_api_ingestion_qa",
            "prod": "adb_api_ingestion_prod",
        }

        return cls(
            name=environment,
            catalog=catalog_mapping.get(
                environment,
                "workspace",
            ),
            audit_schema="audit",
            metadata_schema="config",
            secret_scope="api-ingestion-scope",
        )


environment = Environment.load()