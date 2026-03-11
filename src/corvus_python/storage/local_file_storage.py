import os
from io import BytesIO

from opentelemetry import trace

from ..tracing import (
    add_attributes_to_current_span,
    all_methods_start_new_current_span_with_method_name,
)
from ..storage.file_storage import FileStorage

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class LocalFileStorage(FileStorage):
    def __init__(self, base_path: str):
        super().__init__()
        self._base_path = base_path

    def get_file_bytes(self, filename: str) -> BytesIO:
        path = self._get_file_path(filename)

        with open(path, "rb") as file:
            bytes = BytesIO(file.read())
            bytes.seek(0)
            return bytes

    def get_matching_file_names(self, filename_prefix: str) -> list[str]:
        add_attributes_to_current_span(filename_prefix=filename_prefix)

        full_prefix = os.path.join(self._base_path, filename_prefix)
        parent_folder = os.path.dirname(full_prefix)
        parent_folder_without_prefix = os.path.relpath(parent_folder, self._base_path)

        files_in_target_folder = os.listdir(parent_folder)
        target_file_prefix = os.path.basename(full_prefix)

        return [
            os.path.join(parent_folder_without_prefix, file)
            for file in files_in_target_folder
            if file.startswith(target_file_prefix)
        ]

    def get_latest_matching_file_name(self, filename_prefix: str) -> str:
        add_attributes_to_current_span(filename_prefix=filename_prefix)
        matching_files: list[str] = self.get_matching_file_names(filename_prefix)
        if len(matching_files) == 0:
            raise FileNotFoundError(f"No files found with prefix '{filename_prefix}'")
        latest_file: str = max(matching_files)
        add_attributes_to_current_span(latest_file=latest_file)
        return latest_file

    def get_latest_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        latest_file = self.get_latest_matching_file_name(filename_prefix)
        return self.get_file_bytes(latest_file)

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
        full_name = self.get_single_matching_file_name(filename_prefix)
        return self.get_file_bytes(full_name)

    def write_file(self, file_name: str, file_bytes: bytes) -> None:
        path = self._get_file_path(file_name)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "wb") as file:
            file.write(file_bytes)

    def list_subfolders(self, folder_path: str) -> list[str]:
        if folder_path.startswith("/"):
            folder_path = folder_path[1:]
        abs_path = os.path.join(self._base_path, folder_path)
        if not os.path.exists(abs_path):
            return []
        return [name for name in os.listdir(abs_path) if os.path.isdir(os.path.join(abs_path, name))]

    def _get_file_path(self, filename: str) -> str:
        add_attributes_to_current_span(filename_prefix=filename)

        return os.path.join(self._base_path, filename)
