"""Copyright (c) Endjin Limited. All rights reserved."""

from abc import ABC, abstractmethod
from enum import Enum


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

    :param storage_options: Provider-specific storage options to use when reading or writing data.
    :type storage_options: dict
    """
    def __init__(self, storage_options: dict):
        """Constructor method
        """
        self.storage_options = storage_options

    @abstractmethod
    def get_full_path(self, layer: DataLakeLayer, path: str):
        """Returns the full path to a file in storage.

        :param layer: The layer in the data lake that contains the file.
        :type layer: DataLakeLayer
        :param path: The relative path to the file. This should not have a leading separator.
        :type path: str
        :return: The full path to the file in storage.
        :rtype: str
        """
        pass
