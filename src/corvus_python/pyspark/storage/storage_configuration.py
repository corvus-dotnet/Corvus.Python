"""Copyright (c) Endjin Limited. All rights reserved."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import List


class DataLakeLayer(str, Enum):
    """Enumeration for the different layers in the data lake.
    For more information on medallion architecture, see:
    - https://www.databricks.com/glossary/medallion-architecture
    """
    BRONZE = "bronze",
    SILVER = "silver",
    GOLD = "gold"


class StorageConfiguration(ABC):
    """Base class for a class that provides configuration for persistent storage.

    Attributes:
        storage_options (dict): Provider-specific storage options to use when reading or writing data.
    """
    def __init__(self, storage_options: dict):
        """Constructor method

        Args:
            storage_options (dict): Provider-specific storage options to use when reading or writing data.
        """
        self.storage_options = storage_options

    @abstractmethod
    def get_full_path(self, layer: DataLakeLayer, path: str):
        """Returns the full path to a file in storage.

        Args:
            layer (DataLakeLayer): The layer in the data lake that contains the file.
            path (str): The relative path to the file. This should not have a leading separator.

        Returns:
            str: The full path to the file in storage.
        """
        pass

    @abstractmethod
    def list_files(self, layer: DataLakeLayer, path: str) -> List[str]:
        """Lists all file names in the specified path within the given data lake layer.

        Args:
            layer (DataLakeLayer): The layer in the data lake that contains the files.
            path (str): The relative path to the directory. This should not have a leading separator.

        Returns:
            List[str]: A list of file names in the specified path.
        """
        pass
