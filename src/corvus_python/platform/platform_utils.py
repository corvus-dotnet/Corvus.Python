"""Copyright (c) Endjin Limited. All rights reserved."""

import os

FABRIC = "fabric"
SYNAPSE = "synapse"
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
