import os
from io import BytesIO

from azure.core.credentials import (
    AzureNamedKeyCredential,
    AzureSasCredential,
    TokenCredential,
)
from azure.core.paging import ItemPaged
from azure.storage.blob import BlobPrefix, ContainerClient
from azure.storage.blob._blob_client import BlobClient
from opentelemetry import trace

from ..monitoring import (
    add_attributes_to_current_span,
    all_methods_start_new_current_span_with_method_name,
    start_as_current_span_with_method_name,
)
from ..storage.file_storage import FileStorage

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class AzureBlobFileStorage(FileStorage):
    def __init__(self, container_client: ContainerClient):
        super().__init__()
        self._container_client = container_client

    def get_file_bytes(self, filename: str) -> BytesIO:
        blob = self._container_client.get_blob_client(filename)
        downloader = blob.download_blob()
        bytes = BytesIO()
        downloader.readinto(bytes)
        bytes.seek(0)
        return bytes

    def get_matching_file_names(self, filename_prefix: str) -> list[str]:
        add_attributes_to_current_span(filename_prefix=filename_prefix)

        folder_path = os.path.dirname(filename_prefix)

        matching_files: ItemPaged[str] = self._container_client.list_blob_names(name_starts_with=filename_prefix)

        return [file for file in matching_files if os.path.dirname(file) == folder_path]

    def get_single_matching_file_name(self, filename_prefix: str) -> str:
        return self._get_single_matching_name_for_file_prefix(filename_prefix)

    def get_single_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        full_name = self._get_single_matching_name_for_file_prefix(filename_prefix)
        return self.get_file_bytes(full_name)

    def get_latest_matching_file_name(self, filename_prefix: str) -> str:
        matching_files: list[str] = self.get_matching_file_names(filename_prefix)
        if not matching_files:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")
        latest_file: str = max(matching_files)
        add_attributes_to_current_span(latest_file=latest_file)
        return latest_file

    def get_latest_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        latest_file = self.get_latest_matching_file_name(filename_prefix)
        return self.get_file_bytes(latest_file)

    def write_file(self, file_name: str, file_bytes: bytes) -> None:
        blob: BlobClient = self._container_client.get_blob_client(file_name)
        blob.upload_blob(file_bytes, overwrite=True)

    def list_subfolders(self, folder_path: str) -> list[str]:
        if folder_path and not folder_path.endswith("/"):
            folder_path = folder_path + "/"
        blobs = self._container_client.walk_blobs(name_starts_with=folder_path, delimiter="/")
        return [prefix.name.rstrip("/").split("/")[-1] for prefix in blobs if isinstance(prefix, BlobPrefix)]

    def _get_single_matching_name_for_file_prefix(self, filename_prefix: str) -> str:
        matching_files = self.get_matching_file_names(filename_prefix)

        if len(matching_files) == 0:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")

        if len(matching_files) > 1:
            raise ValueError(
                f"Multiple files found with prefix '{filename_prefix}'. "
                f"Found {len(matching_files)}, expected only 1."
            )

        return matching_files[0]


@start_as_current_span_with_method_name(tracer)
def build_azure_blob_container_client(
    credential: str | dict[str, str] | AzureNamedKeyCredential | AzureSasCredential | TokenCredential | None,
    storage_account_name: str,
    container_name: str,
) -> ContainerClient:

    span = trace.get_current_span()
    span.set_attribute("container_name", container_name)

    account_url = f"https://{storage_account_name}.blob.core.windows.net"

    span.set_attribute("account_url", account_url)

    return ContainerClient(account_url, container_name, credential)
