"""
Deprecated: This module has been moved to corvus_python.storage.

This module provides backward compatibility aliases for the storage configuration classes
that have been moved from corvus_python.pyspark.storage to corvus_python.storage.
New code should import directly from corvus_python.storage.
"""

import warnings

# Re-export everything from the new location
from corvus_python.storage import (  # noqa F401
    StorageConfiguration,
    LocalFileSystemStorageConfiguration,
    AzureDataLakeFileSystemPerLayerConfiguration,
    AzureDataLakeSingleFileSystemConfiguration,
)

# Issue a deprecation warning when this module is imported
warnings.warn(
    "corvus_python.pyspark.storage is deprecated. " "Import from corvus_python.storage instead.",
    DeprecationWarning,
    stacklevel=2,
)
