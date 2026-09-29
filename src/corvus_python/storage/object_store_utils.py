"""Copyright (c) Endjin Limited. All rights reserved."""

import os
from typing import Any, Dict, Optional

import obstore
from obstore.store import LocalStore, ObjectStore, from_url

# obstore's ClientConfig keys. They are declared only in type stubs, so cannot be read at runtime.
_CLIENT_OPTION_KEYS = frozenset(
    {
        "allow_http",
        "allow_invalid_certificates",
        "connect_timeout",
        "default_content_type",
        "default_headers",
        "http1_only",
        "http2_keep_alive_interval",
        "http2_keep_alive_timeout",
        "http2_keep_alive_while_idle",
        "http2_only",
        "pool_idle_timeout",
        "pool_max_idle_per_host",
        "proxy_url",
        "proxy_ca_certificate",
        "proxy_excludes",
        "root_certificate",
        "randomize_addresses",
        "read_timeout",
        "timeout",
        "user_agent",
    }
)


def build_object_store(root: str, storage_options: Optional[Dict[str, Any]], mkdir: bool = False) -> ObjectStore:
    """Builds an obstore store rooted at a path returned by a StorageConfiguration.

    Args:
        root (str): A URL such as abfss://..., or a local file system path.
        storage_options (dict, optional): The configuration's storage_options, as passed to Polars and deltalake.
        mkdir (bool, optional): For local paths, create the root directory if it does not exist.

    Returns:
        ObjectStore: A store whose paths are relative to root.
    """
    # LocalFileSystemStorageConfiguration returns plain OS paths rather than URLs.
    if "://" not in root:
        return LocalStore(root, mkdir=mkdir)
    # Polars and deltalake take store and HTTP client settings in one storage_options dict, but obstore takes client
    # settings separately and panics on unknown store keys, so split them.
    options = storage_options or {}
    client_options: Any = {k: v for k, v in options.items() if k.lower() in _CLIENT_OPTION_KEYS}
    # obstore types config per backend, but the backend is only known once the URL is parsed at runtime.
    config: Any = {k: v for k, v in options.items() if k.lower() not in _CLIENT_OPTION_KEYS}
    return from_url(root, config=config, client_options=client_options or None)


def read_file_bytes(path: str, storage_options: Optional[Dict[str, Any]]) -> bytes:
    """Reads a whole file from a path returned by a StorageConfiguration.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    parent, name = path.rstrip("/").rsplit("/", 1) if "://" in path else os.path.split(path)
    if "://" not in path and not os.path.isdir(parent):
        # LocalStore raises a generic error for a missing root, so match the behaviour for a missing file.
        raise FileNotFoundError(path)
    return bytes(obstore.get(build_object_store(parent, storage_options), name).bytes())
