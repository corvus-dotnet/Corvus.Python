"""Optional persistence of run results to a Delta table.

Spark is imported lazily and a clear error is raised if it is unavailable, so
the rest of the package stays usable from a pure Python notebook.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

__all__ = ["save_results"]


def save_results(result: Any, table_name: str, workspace: Optional[str] = None) -> None:
    """Append ``result.to_dataframe()`` - plus ``run_timestamp`` and
    ``workspace`` columns - to the Delta table ``table_name``."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - requires pyspark
        raise RuntimeError(
            "Persisting results to a Delta table needs pyspark. Install the "
            "'pyspark' extra, or run this cell in a Spark notebook."
        ) from exc

    pdf = result.to_dataframe()
    pdf["run_timestamp"] = datetime.now(timezone.utc).isoformat()
    pdf["workspace"] = workspace

    spark = SparkSession.builder.getOrCreate()
    (spark.createDataFrame(pdf).write.format("delta").mode("append").saveAsTable(table_name))
