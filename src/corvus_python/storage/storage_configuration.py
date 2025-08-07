"""Copyright (c) Endjin Limited. All rights reserved."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional


class DataLakeLayer(str, Enum):
    """Enumeration for the different layers in the data lake.
    For more information on medallion architecture, see:
    - https://www.databricks.com/glossary/medallion-architecture
    """

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class StorageConfiguration(ABC):
    """Base class for a class that provides configuration for persistent storage.

    Attributes:
        storage_options (dict): Provider-specific storage options to use when reading or writing data.
    """

    def __init__(self, storage_options: Optional[Dict[str, Any]]):
        """Constructor method

        Args:
            storage_options (dict): Provider-specific storage options to use when reading or writing data.
        """
        self.storage_options: Optional[Dict[str, Any]] = storage_options

    @abstractmethod
    def get_full_path(self, layer: DataLakeLayer, path: str) -> str:
        """Returns the full path to a file in storage.

        Args:
            layer (DataLakeLayer): The layer in the data lake that contains the file.
            path (str): The relative path to the file. This should not have a leading separator.

        Returns:
            str: The full path to the file in storage.
        """
        pass
