# flake8: noqa
try:
    import pyspark.sql
except ImportError as exc:
    raise ImportError("PySpark is required for corvus-python PySpark utilities. "
                      "Install using corvus-python[pyspark]") from exc

from .sync_tables_locally import sync_synapse_tables_to_local_spark, ObjectSyncDetails