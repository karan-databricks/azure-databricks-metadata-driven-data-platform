# src/enterprise_api_ingestion_platform/ingestion/ingestion_service.py

from datetime import datetime, timezone
from uuid import uuid4

from enterprise_api_ingestion_platform.checkpoint.checkpoint_service import (
    CheckpointService,
)
from enterprise_api_ingestion_platform.landing.landing_service import (
    LandingService,
)
from enterprise_api_ingestion_platform.logging.audit_logger import (
    AuditLogger,
)
from enterprise_api_ingestion_platform.logging.logger import (
    get_logger,
)
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class IngestionService:
    """
    Application service that orchestrates a complete ingestion run.
    """

    def __init__(
        self,
        landing_service: LandingService,
        audit_logger: AuditLogger,
        checkpoint_service: CheckpointService,
    ) -> None:
        self._landing_service = landing_service
        self._audit_logger = audit_logger
        self._checkpoint_service = checkpoint_service
        self._logger = get_logger(__name__)

    def run(
        self,
        metadata: ApiMetadata,
    ) -> None:
        """
        Executes a single API ingestion.

        Parameters
        ----------
        metadata
            API metadata configuration.
        """

        run_id = str(uuid4())

        start_time = datetime.now(timezone.utc)

        status = "SUCCESS"

        landing_file: str | None = None

        error_message: str | None = None

        self._logger.info(
            "Starting ingestion run. run_id=%s api_name=%s",
            run_id,
            metadata.api_name,
        )

        try:
            initial_cursor = self._checkpoint_service.get_effective_cursor(
                metadata,
            )

            landing_result = self._landing_service.ingest(
                metadata,
                initial_cursor=initial_cursor,
            )

            landing_file = landing_result.landing_file

            self._logger.info(
                "Landing completed. run_id=%s landing_file=%s",
                run_id,
                landing_file,
            )

            self._checkpoint_service.save_checkpoint(
                metadata=metadata,
                checkpoint_state=landing_result.checkpoint_state,
                run_id=run_id,
                recorded_at=datetime.now(timezone.utc),
            )

        except Exception as ex:
            status = "FAILED"

            error_message = str(ex)

            self._logger.exception(
                "Ingestion failed. run_id=%s api_name=%s",
                run_id,
                metadata.api_name,
            )

            raise

        finally:
            end_time = datetime.now(timezone.utc)

            duration_seconds = (
                end_time - start_time
            ).total_seconds()

            self._logger.info(
                "Writing audit record. run_id=%s status=%s duration_seconds=%.2f",
                run_id,
                status,
                duration_seconds,
            )

            self._audit_logger.log_execution(
                run_id=run_id,
                api_name=metadata.api_name,
                job_name="generic_api_ingestion_job",
                start_time=start_time,
                end_time=end_time,
                status=status,
                records_read=None,
                records_written=None,
                duration_seconds=duration_seconds,
                error_message=error_message,
                landing_file=landing_file,
            )

            self._logger.info(
                "Ingestion run completed. run_id=%s status=%s",
                run_id,
                status,
            )