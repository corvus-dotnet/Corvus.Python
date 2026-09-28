"""Copyright (c) Endjin Limited. All rights reserved."""

from abc import abstractmethod
from typing import Any, Dict, Optional

from corvus_python.fabric import configure_tls_trust_store

from .storage_configuration import DataLakeLayer, StorageConfiguration

ONELAKE_DFS_ENDPOINT = "onelake.dfs.fabric.microsoft.com"


class FabricLakehousePerLayerConfiguration(StorageConfiguration):
    """Base class for StorageConfigurations that use Microsoft Fabric OneLake and assume that there is a separate
    Lakehouse for each layer, named 'bronze', 'silver' and 'gold' by default.

    Each subclass targets one area of the Lakehouse: FabricLakehouseTablesConfiguration for managed Delta tables and
    FabricLakehouseFilesConfiguration for unmanaged files.

    Attributes:
        workspace_name (str): The name of the Fabric workspace containing the Lakehouses.
        workspace_names (dict): The workspace name to use for each layer.
        lakehouse_names (dict): The Lakehouse name to use for each layer.
    """

    @property
    @abstractmethod
    def lakehouse_area(self) -> str:
        """The top-level area of the Lakehouse that paths are rooted in: 'Tables' or 'Files'."""

    def __init__(
        self,
        workspace_name: str,
        lakehouse_names: Optional[Dict[DataLakeLayer, str]] = None,
        workspace_names: Optional[Dict[DataLakeLayer, str]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """Constructor method

        Args:
            workspace_name (str): The name of the Fabric workspace containing the Lakehouses.
            lakehouse_names (dict, optional): Overrides for the Lakehouse name used for a layer. Layers not specified
                use a Lakehouse named after the layer.
            workspace_names (dict, optional): Overrides for the workspace used for a layer, for when Lakehouses live in
                different workspaces. Layers not specified use workspace_name.
            storage_options (dict, optional): Provider-specific storage options to use when reading or writing data.
        """
        super().__init__(storage_options)
        self.workspace_name = workspace_name
        self.workspace_names = {layer: workspace_name for layer in DataLakeLayer} | _normalise_layer_keys(
            workspace_names
        )
        self.lakehouse_names = {layer: str(layer) for layer in DataLakeLayer} | _normalise_layer_keys(lakehouse_names)
        # object_store builds its TLS config once per process, so this must precede any request.
        configure_tls_trust_store()

    def get_full_path(self, layer: DataLakeLayer, path: str) -> str:
        layer = DataLakeLayer(layer)
        workspace = self.workspace_names[layer]
        lakehouse = self.lakehouse_names[layer]
        # Callers such as PolarsDeltaTableRepository build "{base_path}/..." so an empty base_path yields a leading
        # separator, which would otherwise produce an empty path segment after the Lakehouse area.
        relative_path = path.lstrip("/")
        return f"abfss://{workspace}@{ONELAKE_DFS_ENDPOINT}/{lakehouse}.Lakehouse/{self.lakehouse_area}/{relative_path}"


class FabricLakehouseTablesConfiguration(FabricLakehousePerLayerConfiguration):
    """Fabric Lakehouse configuration targeting the managed 'Tables' area, for Delta tables.

    Fabric only discovers tables at Tables/<table> or, in schema-enabled Lakehouses, Tables/<schema>/<table>. With
    PolarsDeltaTableRepository, use an empty base_path so that the database name becomes the schema.
    """

    lakehouse_area = "Tables"


class FabricLakehouseFilesConfiguration(FabricLakehousePerLayerConfiguration):
    """Fabric Lakehouse configuration targeting the unmanaged 'Files' area, for raw files such as CSV, JSON or Excel."""

    lakehouse_area = "Files"


def _normalise_layer_keys(overrides: Optional[Dict[DataLakeLayer, str]]) -> Dict[DataLakeLayer, str]:
    # DataLakeLayer() raises ValueError for unknown layers, so typos fail at construction rather than lookup.
    return {DataLakeLayer(layer): name for layer, name in (overrides or {}).items()}
