"""
Deprecated: This module has been moved to corvus_python.spark_utils.

This module provides backward compatibility aliases for the storage configuration classes
that have been moved from corvus_python.pyspark.storage to corvus_python.spark_utils.
New code should import directly from corvus_python.spark_utils.
"""

import warnings

# Re-export everything from the new location
from corvus_python.spark_utils import get_spark_utils  # noqa F401

# Issue a deprecation warning when this module is imported
warnings.warn(
    "get_spark_utils in corvus_python.pyspark.utilities is deprecated. "
    "Import from corvus_python.spark_utils instead.",
    DeprecationWarning,
    stacklevel=2,
)
