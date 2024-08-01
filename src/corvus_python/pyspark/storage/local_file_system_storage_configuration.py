"""Copyright (c) Endjin Limited. All rights reserved."""

import os
from .storage_configuration import DataLakeLayer, StorageConfiguration


class LocalFileSystemStorageConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses the local file system.

    Attributes:
        base_path (str): The base path to use for the local file system. This should not have a trailing separator.
    """
    def __init__(self, base_path: str):
        """Constructor method

        Args:
            base_path (str): The base path to use for the local file system. This should not have a trailing separator.
        """
        super().__init__(None)
        self.base_path = os.path.abspath(base_path)

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"{self.base_path}/{layer}/{path}"
    
    def list_files(self, layer: DataLakeLayer, path: str):
        return os.listdir(f"{self.base_path}/{layer}/{path}")
