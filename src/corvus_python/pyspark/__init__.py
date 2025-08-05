# flake8: noqa
try:
    import pyspark.sql
except ImportError as exc:
    raise ImportError(
        "PySpark is required for corvus-python PySpark utilities. " "Install using corvus-python[pyspark]"
    ) from exc
