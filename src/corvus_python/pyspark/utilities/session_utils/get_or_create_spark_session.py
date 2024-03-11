"""Copyright (c) Endjin Limited. All rights reserved."""

from pyspark.sql import SparkSession
import os
from .local_spark_session import LocalSparkSessionConfig, LocalSparkSession


def get_or_create_spark_session(
        local_spark_session_config: LocalSparkSessionConfig = None
        ) -> SparkSession:
    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = LocalSparkSession(local_spark_session_config).create_spark_session()

    return spark
