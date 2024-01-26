"""Copyright (c) Endjin Limited. All rights reserved."""

from .storage_configuration import DataLakeLayer, StorageConfiguration


class AzureDataLakeFileSystemPerLayerConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses Azure Data Lake Gen 2 and assumes that there is a separate
    ADLS file system for each layer, named 'bronze', 'silver' and 'gold'.

    :param storage_account_name: The name of the storage account.
    :type storage_account_name: str
    :param storage_options: Provider-specific storage options to use when reading or writing data.
    :type storage_options: dict
    """
    def __init__(
            self,
            storage_account_name: str,
            storage_options: dict = None):
        """Constructor method
        """

        super().__init__(storage_options)
        self.storage_account_name = storage_account_name

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"abfss://{layer}@{self.storage_account_name}.dfs.core.windows.net/{path}"


class AzureDataLakeSingleFileSystemConfiguration(StorageConfiguration):
    """Implementation of StorageConfiguration that uses Azure Data Lake Gen 2 and assumes that there is a single
    ADLS file system containing top level folders for each layer, named 'bronze', 'silver' and 'gold'.

    :param storage_account_name: The name of the storage account.
    :type storage_account_name: str
    :param file_system_name: The name of the file system.
    :type file_system_name: str
    :param storage_options: Provider-specific storage options to use when reading or writing data.
    :type storage_options: dict
    """
    def __init__(
            self,
            storage_account_name: str,
            file_system_name: str,
            storage_options: dict = None):
        """Constructor method
        """
        super().__init__(storage_options)
        self.storage_account_name = storage_account_name
        self.file_system_name = file_system_name

    def get_full_path(self, layer: DataLakeLayer, path: str):
        return f"abfss://{self.file_system_name}@{self.storage_account_name}.dfs.core.windows.net/{layer}/{path}"
