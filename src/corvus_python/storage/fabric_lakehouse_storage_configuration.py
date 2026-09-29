"""Copyright (c) Endjin Limited. All rights reserved."""

import logging
import uuid
from abc import abstractmethod
from typing import Any, Dict, Optional

from corvus_python.fabric import configure_tls_trust_store
from corvus_python.platform import FABRIC, get_platform, is_spark_runtime

from .storage_configuration import DataLakeLayer, StorageConfiguration

ONELAKE_DFS_ENDPOINT = "onelake.dfs.fabric.microsoft.com"

logger = logging.getLogger(__name__)


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
        allow_invalid_certificates: bool = False,
    ):
        """Constructor method

        Args:
            workspace_name (str): The name of the Fabric workspace containing the Lakehouses.
            lakehouse_names (dict, optional): Overrides for the Lakehouse name used for a layer. Layers not specified
                use a Lakehouse named after the layer.
            workspace_names (dict, optional): Overrides for the workspace used for a layer, for when Lakehouses live in
                different workspaces. Layers not specified use workspace_name.
            storage_options (dict, optional): Provider-specific storage options to use when reading or writing data.
            allow_invalid_certificates (bool, optional): Disable TLS certificate validation for OneLake requests made
                through storage_options (Polars, deltalake, obstore). Only takes effect on the Fabric Spark runtime,
                which routes OneLake traffic through a proxy presenting a self-signed CA certificate that rustls
                rejects with CaUsedAsEndEntity. It is ignored elsewhere, including the Fabric Python runtime, which
                does not need it. This removes protection against interception, so enable it only where needed.
        """
        if allow_invalid_certificates:
            storage_options = _with_invalid_certificates_allowed(storage_options)
        super().__init__(storage_options)
        self.workspace_name = workspace_name
        self.workspace_names = {layer: workspace_name for layer in DataLakeLayer} | _normalise_layer_keys(
            workspace_names
        )
        self.lakehouse_names = {layer: str(layer) for layer in DataLakeLayer} | _normalise_layer_keys(lakehouse_names)
        _ensure_names_not_ids(self.workspace_names, "workspace")
        _ensure_names_not_ids(self.lakehouse_names, "Lakehouse")
        # object_store reads SSL_CERT_FILE whenever it builds an HTTP client, so this must precede any request.
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


def _with_invalid_certificates_allowed(storage_options: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    platform = get_platform()
    spark = is_spark_runtime()
    if platform != FABRIC or not spark:
        logger.info(
            "allow_invalid_certificates ignored: only needed on the Fabric Spark runtime (platform=%s, spark=%s).",
            platform,
            spark,
        )
        return storage_options
    logger.warning("TLS certificate validation is disabled for OneLake requests (allow_invalid_certificates).")
    # Copied so the caller's dict is not mutated.
    return {**(storage_options or {}), "allow_invalid_certificates": "true"}


def _ensure_names_not_ids(names: Dict[DataLakeLayer, str], item: str) -> None:
    # OneLake rejects paths that mix IDs with names (400 Bad Request), and Lakehouses are always addressed by name
    # here, so an ID would only fail later with an unhelpful error.
    for layer, name in names.items():
        try:
            uuid.UUID(name)
        except ValueError:
            continue
        raise ValueError(
            f"The {item} for layer '{layer}' looks like an ID ('{name}'). Use the {item} name instead: OneLake does "
            "not accept paths that mix IDs and names."
        )


def _normalise_layer_keys(overrides: Optional[Dict[DataLakeLayer, str]]) -> Dict[DataLakeLayer, str]:
    # DataLakeLayer() raises ValueError for unknown layers, so typos fail at construction rather than lookup.
    return {DataLakeLayer(layer): name for layer, name in (overrides or {}).items()}
