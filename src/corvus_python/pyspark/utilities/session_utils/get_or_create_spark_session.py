"""Copyright (c) Endjin Limited. All rights reserved."""

import os

try:
    from pyspark.sql import SparkSession
except ImportError as exc:
    raise ImportError("PySpark is required for get_or_create_spark_session functionality. "
                      "Install using corvus-python[pyspark]") from exc

from .local_spark_session import LocalSparkSessionConfig, LocalSparkSession


def get_or_create_spark_session(
        local_spark_session_config: LocalSparkSessionConfig = None
        ):
    """
    Get or create a Spark session. Two runtimes are currently supported: Synapse and Local.

    Args:
        local_spark_session_config (LocalSparkSessionConfig, optional): The configuration for the local Spark session.
            Defaults to None.
    """

    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = LocalSparkSession(local_spark_session_config).create_spark_session()

    return spark
