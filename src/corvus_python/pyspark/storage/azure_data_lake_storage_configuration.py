"""
Deprecated: This module has been moved to corvus_python.storage.azure_data_lake_storage_configuration.

This module provides backward compatibility for imports that were previously
available at corvus_python.pyspark.storage.azure_data_lake_storage_configuration.
"""

import warnings
from corvus_python.storage.azure_data_lake_storage_configuration import *  # noqa F401, F403

warnings.warn(
    "corvus_python.pyspark.storage.azure_data_lake_storage_configuration is deprecated. "
    "Import from corvus_python.storage.azure_data_lake_storage_configuration instead.",
    DeprecationWarning,
    stacklevel=2,
)
