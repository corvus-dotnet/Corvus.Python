"""Copyright (c) Endjin Limited. All rights reserved."""

from .storage_configuration import DataLakeLayer, StorageConfiguration
from utilities import get_spark_utils
from typing import List


class AzureDataLakeFileSystemPerLayerConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses Azure Data Lake Gen 2 and assumes that there is a separate
    ADLS file system for each layer, named 'bronze', 'silver' and 'gold'.

    Attributes:
        storage_account_name (str): The name of the storage account.
    """
    def __init__(
            self,
            storage_account_name: str,
            storage_options: dict = None):
        """Constructor method

        Args:
            storage_account_name (str): The name of the storage account.
            storage_options (dict, optional): Provider-specific storage options to use when reading or writing data.
        """

        super().__init__(storage_options)
        self.storage_account_name = storage_account_name

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"abfss://{layer}@{self.storage_account_name}.dfs.core.windows.net/{path}"
    
    def list_files(self, layer: DataLakeLayer, path: str) -> List[str]:
        spark_utils = get_spark_utils()
        files = spark_utils.fs.ls(self.get_full_path(layer, path))
        return [file.name for file in files]
        



class AzureDataLakeSingleFileSystemConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses Azure Data Lake Gen 2 and assumes that there is a single
    ADLS file system containing top level folders for each layer, named 'bronze', 'silver' and 'gold'.

    Attributes:
        storage_account_name (str): The name of the storage account.
        file_system_name (str): The name of the file system.
    """
    def __init__(
            self,
            storage_account_name: str,
            file_system_name: str,
            storage_options: dict = None):
        """Constructor method

        Args:
            storage_account_name (str): The name of the storage account.
            file_system_name (str): The name of the file system.
            storage_options (dict, optional): Provider-specific storage options to use when reading or writing data.
        """
        super().__init__(storage_options)
        self.storage_account_name = storage_account_name
        self.file_system_name = file_system_name

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"abfss://{self.file_system_name}@{self.storage_account_name}.dfs.core.windows.net/{layer}/{path}"
