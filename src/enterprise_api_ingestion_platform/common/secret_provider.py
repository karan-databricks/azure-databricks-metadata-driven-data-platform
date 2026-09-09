from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class SecretProvider:
    """
    Provides access to secrets stored in a Databricks Secret Scope.
    """

    @staticmethod
    def get(
        scope: str,
        key: str,
    ) -> str:
        """
        Returns a secret value from the specified Databricks Secret Scope.
        """

        spark = SparkSession.getActiveSession()

        if spark is None:
            raise RuntimeError(
                "No active SparkSession found."
            )

        dbutils = DBUtils(spark)

        return dbutils.secrets.get(
            scope=scope,
            key=key,
        )