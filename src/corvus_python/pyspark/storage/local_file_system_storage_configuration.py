"""Copyright (c) Endjin Limited. All rights reserved."""

import os
from .storage_configuration import DataLakeLayer, StorageConfiguration


class LocalFileSystemStorageConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses the local file system.
    Data will be stored in the specified base path, with a subfolder for each layer.

    :param base_path: The base path to use for the local file system. This should not have a trailing separator.
    """
    def __init__(self, base_path: str):
        """Constructor method
        """
        super().__init__(None)
        self.base_path = os.path.abspath(base_path)

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"{self.base_path}/{layer}/{path}"
