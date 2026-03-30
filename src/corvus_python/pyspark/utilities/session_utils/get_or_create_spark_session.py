"""Copyright (c) Endjin Limited. All rights reserved."""

from typing import Optional
from pyspark.sql import SparkSession
import os
from .local_spark_session import LocalSparkSessionConfig, LocalSparkSession


def get_or_create_spark_session(local_spark_session_config: Optional[LocalSparkSessionConfig] = None) -> SparkSession:
    """
    Get or create a Spark session. Two runtimes are currently supported: Synapse and Local.

    Args:
        local_spark_session_config (LocalSparkSessionConfig, optional): The configuration for the local Spark session.
            Defaults to None.
    """
    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        spark = SparkSession.builder.getOrCreate()  # type: ignore
    else:
        if local_spark_session_config is None:
            raise ValueError("local_spark_session_config must be provided when not running in Synapse environment.")
        spark = LocalSparkSession(local_spark_session_config).create_spark_session()

    return spark
