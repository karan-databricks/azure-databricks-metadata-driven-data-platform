"""
Custom exceptions for the Enterprise API Ingestion Platform.
"""

class UnsupportedPaginationTypeException(Exception):
    """Raised when an unsupported pagination type is configured."""


class MetadataNotFoundException(Exception):
    """Raised when no metadata exists for the requested API."""


class DuplicateMetadataException(Exception):
    """Raised when multiple metadata records exist for the same API."""


class AuthenticationException(Exception):
    """Raised when authentication fails."""


class ApiRequestException(Exception):
    """Raised when an API request fails."""


class RateLimitException(Exception):
    """Raised when an API rate limit is reached."""


class PaginationException(Exception):
    """Raised when pagination fails."""


class BronzeWriteException(Exception):
    """Raised when writing to the Bronze layer fails."""


class AuditLoggingException(Exception):
    """Raised when audit logging fails."""