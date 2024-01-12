"""Copyright (c) Endjin Limited. All rights reserved."""

from abc import ABC, abstractmethod
from enum import Enum


class DataLakeLayer(str, Enum):
    """Enumeration for the different layers in the data lake.
    For more information on medallion architecture, see:
    - https://www.databricks.com/glossary/medallion-architecture
    - https://www.databricks.com/glossary/medallion-architecture"""
    BRONZE = "bronze",
    SILVER = "silver",
    GOLD = "gold"


class StorageConfiguration(ABC):
    """Base class for a class that provides configuration for persistent storage."""
    def __init__(self, storage_options: dict):
        """Initializes a new instance of the StorageConfiguration class.

        Args:
            storage_options (dict): Provider-specific storage options to use when reading or writing data."""
        self.storage_options = storage_options

    @abstractmethod
    def get_full_path(self, layer: DataLakeLayer, path: str):
        """Returns the full path to a file in storage.

        Args:
            layer (DataLakeLayer): The layer in the data lake that contains the file.
            path (str): The relative path to the file. This should not have a leading separator"""
        pass
