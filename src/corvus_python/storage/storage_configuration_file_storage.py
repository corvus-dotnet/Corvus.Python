"""Copyright (c) Endjin Limited. All rights reserved."""

import posixpath
from io import BytesIO
from typing import Any, Dict, Optional

import obstore
from obstore.store import LocalStore, ObjectStore, from_url
from opentelemetry import trace

from ..monitoring import (
    add_attributes_to_current_span,
    all_methods_start_new_current_span_with_method_name,
)
from .file_storage import FileStorage
from .storage_configuration import DataLakeLayer, StorageConfiguration

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class StorageConfigurationFileStorage(FileStorage):
    """FileStorage rooted at one layer of a StorageConfiguration.

    I/O goes through obstore, which uses the same object_store library as Polars and deltalake, so the
    configuration's storage_options work unchanged and any backend it supports (local, ADLS, Fabric OneLake) is
    available. File names are relative to the layer root, e.g. 'raw/orders_2026-01-01.csv'.
    """

    def __init__(self, storage_configuration: StorageConfiguration, layer: DataLakeLayer):
        """Constructor method

        Args:
            storage_configuration (StorageConfiguration): The configuration providing the root path and storage
                options.
            layer (DataLakeLayer): The layer to root file names in.
        """
        super().__init__()
        self._root = storage_configuration.get_full_path(layer, "").rstrip("/\\")
        add_attributes_to_current_span(root=self._root)
        self._store = _build_store(self._root, storage_configuration.storage_options)

    def get_file_bytes(self, filename: str) -> BytesIO:
        add_attributes_to_current_span(filename=filename)
        result = BytesIO(bytes(obstore.get(self._store, filename).bytes()))
        result.seek(0)
        return result

    def get_matching_file_names(self, filename_prefix: str) -> list[str]:
        add_attributes_to_current_span(filename_prefix=filename_prefix)

        folder_path, file_prefix = posixpath.split(filename_prefix)
        listing = obstore.list_with_delimiter(self._store, folder_path or None)

        return [obj["path"] for obj in listing["objects"] if posixpath.basename(obj["path"]).startswith(file_prefix)]

    def get_latest_matching_file_name(self, filename_prefix: str) -> str:
        matching_files = self.get_matching_file_names(filename_prefix)
        if not matching_files:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")
        latest_file = max(matching_files)
        add_attributes_to_current_span(latest_file=latest_file)
        return latest_file

    def get_latest_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        return self.get_file_bytes(self.get_latest_matching_file_name(filename_prefix))

    def get_single_matching_file_name(self, filename_prefix: str) -> str:
        matching_files = self.get_matching_file_names(filename_prefix)

        if len(matching_files) == 0:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")

        if len(matching_files) > 1:
            raise ValueError(
                f"Multiple files found with prefix '{filename_prefix}'. "
                f"Found {len(matching_files)}, expected only 1."
            )

        return matching_files[0]

    def get_single_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        return self.get_file_bytes(self.get_single_matching_file_name(filename_prefix))

    def write_file(self, file_name: str, file_bytes: bytes) -> None:
        add_attributes_to_current_span(filename=file_name)
        obstore.put(self._store, file_name, file_bytes)

    def list_subfolders(self, folder_path: str) -> list[str]:
        listing = obstore.list_with_delimiter(self._store, folder_path.strip("/") or None)
        return [posixpath.basename(prefix) for prefix in listing["common_prefixes"]]


def _build_store(root: str, storage_options: Optional[Dict[str, Any]]) -> ObjectStore:
    # LocalFileSystemStorageConfiguration returns plain OS paths rather than URLs.
    if "://" not in root:
        return LocalStore(root, mkdir=True)
    # obstore types config per backend, but the backend is only known once the URL is parsed at runtime.
    config: Any = storage_options or {}
    return from_url(root, config=config)
