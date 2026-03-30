"""
Deprecated: This module has been moved to corvus_python.spark_utils.

This module provides backward compatibility aliases for the spark utility functions
that have been moved from corvus_python.pyspark.utilities to corvus_python.spark_utils.
New code should import directly from corvus_python.spark_utils.
"""

import warnings

from corvus_python.spark_utils.spark_utils import *  # noqa F401

# Issue a deprecation warning when this module is imported
warnings.warn(
    "corvus_python.pyspark.utilities.spark_utils.spark_utils is deprecated. "
    "Import from corvus_python.spark_utils instead.",
    DeprecationWarning,
    stacklevel=2,
)
