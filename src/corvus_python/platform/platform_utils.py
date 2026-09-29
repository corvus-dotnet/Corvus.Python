"""Copyright (c) Endjin Limited. All rights reserved."""

import os
import sys

FABRIC = "fabric"
SYNAPSE = "synapse"
# Neither a Synapse nor a Fabric notebook: local development, but also hosted runtimes such as
# Azure Container Apps. Locally there is nothing to detect, so this is returned whichever platform
# the code will eventually run on.
LOCAL = "local"


def get_platform() -> str:
    """Return 'fabric', 'synapse' or 'local'. Requires no Spark session."""
    try:
        import notebookutils
    except ImportError:
        return LOCAL

    ctx = getattr(notebookutils.runtime, "context", None) or {}
    if ctx.get("productType") == "Fabric":
        return FABRIC

    # Only reachable once Fabric is positively excluded: Fabric Spark sessions
    # also set MMLSPARK_PLATFORM_INFO=synapse, so this is unsafe as a first check.
    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        return SYNAPSE

    # Defaults to LOCAL rather than SYNAPSE: dummy-notebookutils ships an empty
    # runtime.context, so a synapse default would misroute if it reached ACA.
    return LOCAL


def is_spark_runtime() -> bool:
    """Return True if a Spark session is running in this process, e.g. a Spark notebook rather than a Fabric Python
    notebook. Never starts Spark or imports pyspark."""
    # A running session means pyspark is already imported, so checking sys.modules avoids a slow import elsewhere.
    pyspark = sys.modules.get("pyspark")
    if pyspark is None:
        return False
    # SparkContext._active_spark_context is process-wide, unlike SparkSession.getActiveSession(), which is per thread.
    return getattr(getattr(pyspark, "SparkContext", None), "_active_spark_context", None) is not None
