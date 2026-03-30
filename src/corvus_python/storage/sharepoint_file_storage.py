import os
from io import BytesIO
from typing import Any

import requests
from opentelemetry import trace

from ..sharepoint import SharePointUtilities
from ..monitoring import (
    add_attributes_to_current_span,
    all_methods_start_new_current_span_with_method_name,
)
from ..storage.file_storage import FileStorage

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class SharePointFileStorage(FileStorage):
    def __init__(
        self, sharepoint_tenant_fqdn: str, sharepoint_site_name: str, library_name: str, auth_token: str
    ) -> None:
        super().__init__()
        self._sharepoint_tenant_fqdn = sharepoint_tenant_fqdn
        self._sharepoint_site_name = sharepoint_site_name
        self._library_name = library_name
        self._auth_token = auth_token
        self._headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {auth_token}",
        }
        self.drive_id: str = SharePointUtilities.get_drive_id(
            sharepoint_tenant_fqdn=self._sharepoint_tenant_fqdn,
            sharepoint_site_name=self._sharepoint_site_name,
            library_name=self._library_name,
            headers=self._headers,
        )

    def get_file_bytes(self, filename: str) -> BytesIO:
        download_url = self._get_file_path(filename)
        response = requests.get(download_url)
        response.raise_for_status()
        bytes_io = BytesIO(response.content)
        bytes_io.seek(0)
        return bytes_io

    def get_matching_file_names(self, filename_prefix: str) -> list[str]:
        add_attributes_to_current_span(filename_prefix=filename_prefix)

        # Parse the prefix to extract folder path and file prefix
        folder_path = os.path.dirname(filename_prefix)
        file_prefix = os.path.basename(filename_prefix)

        # Get all files in the folder
        files_in_folder_response = SharePointUtilities.get_download_urls_for_files_in_folder(
            drive_id=self.drive_id,
            folder_name=folder_path if folder_path else "",
            token=self._auth_token,
        )
        # Type assertion: The method returns a list despite its return type annotation
        files_in_folder: list[dict[str, Any]] = files_in_folder_response  # type: ignore

        # Filter files that match the prefix
        matching_files = [
            os.path.join(folder_path, str(file["name"])) if folder_path else str(file["name"])
            for file in files_in_folder
            if str(file["name"]).startswith(file_prefix) and "file" in file
        ]

        return matching_files

    def get_latest_matching_file_name(self, filename_prefix: str) -> str:
        matching_files = self.get_matching_file_names(filename_prefix)

        if not matching_files:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")

        latest_file = max(matching_files)
        add_attributes_to_current_span(latest_file=latest_file)
        return latest_file

    def get_latest_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        latest_file = self.get_latest_matching_file_name(filename_prefix)
        return self.get_file_bytes(latest_file)

    def get_single_matching_file_name(self, filename_prefix: str) -> str:
        return self._get_single_matching_file_name_for_prefix(filename_prefix)

    def get_single_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        file_name = self._get_single_matching_file_name_for_prefix(filename_prefix)
        return self.get_file_bytes(file_name)

    def write_file(self, file_name: str, file_bytes: bytes) -> None:
        SharePointUtilities.save_file(self.drive_id, file_name, self._auth_token, bytearray(file_bytes))

    def list_subfolders(self, folder_path: str) -> list[str]:
        items = SharePointUtilities.get_items_in_folder(
            drive_id=self.drive_id, folder_path=folder_path, token=self._auth_token
        )
        folders: list[dict[str, str]] = [item for item in items if "folder" in item]  # type: ignore
        return [folder["webUrl"].split("/")[-1] for folder in folders]

    def _get_single_matching_file_name_for_prefix(self, filename_prefix: str) -> str:
        matching_files = self.get_matching_file_names(filename_prefix)

        if len(matching_files) == 0:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")

        if len(matching_files) > 1:
            raise ValueError(
                f"Multiple files found with prefix '{filename_prefix}'. "
                f"Found {len(matching_files)}, expected only 1."
            )

        return matching_files[0]

    def _get_file_path(self, filename: str) -> str:
        return SharePointUtilities.get_file_download_url(
            drive_id=self.drive_id,
            file_name=filename,
            token=self._auth_token,
        )
