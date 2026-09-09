from abc import ABC, abstractmethod
from typing import Dict


class AuthProvider(ABC):
    """
    Base class for all authentication providers.
    """

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """
        Returns the HTTP headers required for authentication.
        """

        raise NotImplementedError

    def get_query_parameters(self) -> Dict[str, str]:
        """
        Returns query parameters required for authentication.

        Providers that authenticate using HTTP headers do not need
        to override this method.
        """

        return {}