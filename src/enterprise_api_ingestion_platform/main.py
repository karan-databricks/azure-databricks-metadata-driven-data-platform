# src/enterprise_api_ingestion_platform/main.py

import argparse
from importlib.metadata import PackageNotFoundError, version

import enterprise_api_ingestion_platform
from pyspark.sql import SparkSession

from enterprise_api_ingestion_platform.checkpoint.checkpoint_repository import (
    CheckpointRepository,
)
from enterprise_api_ingestion_platform.checkpoint.checkpoint_service import (
    CheckpointService,
)
from enterprise_api_ingestion_platform.execution.execution_service import (
    ExecutionService,
)
from enterprise_api_ingestion_platform.ingestion.ingestion_service import (
    IngestionService,
)
from enterprise_api_ingestion_platform.landing.landing_service import (
    LandingService,
)
from enterprise_api_ingestion_platform.landing.landing_writer import (
    LandingWriter,
)
from enterprise_api_ingestion_platform.logging.audit_logger import (
    AuditLogger,
)
from enterprise_api_ingestion_platform.logging.logger import (
    get_logger,
)
from enterprise_api_ingestion_platform.metadata.metadata_service import (
    MetadataService,
)
from enterprise_api_ingestion_platform.storage.storage_writer_factory import (
    StorageWriterFactory,
)
from enterprise_api_ingestion_platform.utils.http_client import (
    HttpClient,
)

ALL_APIS = "ALL"


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments supplied by the Databricks Job.
    """

    parser = argparse.ArgumentParser(
        description="Enterprise API Ingestion Platform",
    )

    parser.add_argument(
        "--api_name",
        required=True,
        help=(
            "Logical API name configured in metadata "
            "or ALL."
        ),
    )

    parser.add_argument(
        "--landing_catalog",
        required=True,
        help="Unity Catalog catalog containing the landing volume.",
    )

    parser.add_argument(
        "--landing_schema",
        required=True,
        help="Unity Catalog schema containing the landing volume.",
    )

    parser.add_argument(
        "--landing_volume",
        required=True,
        help="Unity Catalog landing volume name.",
    )

    return parser.parse_args()


def main() -> None:
    """
    Application entry point.
    """

    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("Enterprise API Ingestion Platform")
        .getOrCreate()
    )

    logger = get_logger(__name__)

    try:
        package_version = version(
            "enterprise_api_ingestion_platform",
        )
    except PackageNotFoundError:
        package_version = "unknown"

    logger.info(
        "Package version: %s",
        package_version,
    )

    logger.info(
        "Package location: %s",
        enterprise_api_ingestion_platform.__file__,
    )

    logger.info(
        "LandingWriter methods: %s",
        [
            name
            for name in dir(LandingWriter)
            if not name.startswith("_")
        ],
    )

    metadata_service = MetadataService()
    http_client = HttpClient()

    storage_writer = StorageWriterFactory.create(
        spark=spark,
        landing_catalog=args.landing_catalog,
        landing_schema=args.landing_schema,
        landing_volume=args.landing_volume,
    )

    logger.info(
        "StorageWriter implementation: %s",
        type(storage_writer).__name__,
    )

    logger.info(
        "StorageWriter module: %s",
        type(storage_writer).__module__,
    )

    landing_writer = LandingWriter(
        storage_writer,
    )

    logger.info(
        "LandingWriter instance created successfully.",
    )

    landing_service = LandingService(
        http_client=http_client,
        landing_writer=landing_writer,
    )

    audit_logger = AuditLogger(
        spark,
    )

    checkpoint_repository = CheckpointRepository(
        spark,
    )

    checkpoint_service = CheckpointService(
        checkpoint_repository,
    )

    ingestion_service = IngestionService(
        landing_service=landing_service,
        audit_logger=audit_logger,
        checkpoint_service=checkpoint_service,
    )

    execution_service = ExecutionService(
        metadata_service=metadata_service,
        ingestion_service=ingestion_service,
    )

    if args.api_name.upper() == ALL_APIS:
        logger.info(
            "Executing all enabled APIs.",
        )
        execution_service.execute_all()
    else:
        logger.info(
            "Executing API: %s",
            args.api_name,
        )

        metadata = metadata_service.get_metadata(
            args.api_name,
        )

        execution_service.execute_api(
            metadata,
        )

        logger.info(
            "Execution completed successfully.",
        )


if __name__ == "__main__":
    main()