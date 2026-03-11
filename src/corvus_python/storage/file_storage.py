from abc import ABC
from io import BytesIO


class FileStorage(ABC):
    def __init__(self) -> None:
        pass

    def get_file_bytes(self, filename: str) -> BytesIO:
        raise NotImplementedError("get_file_bytes method not implemented")

    def get_matching_file_names(self, filename_prefix: str) -> list[str]:
        raise NotImplementedError("get_matching_file_names method not implemented")

    def get_latest_matching_file_name(self, filename_prefix: str) -> str:
        raise NotImplementedError("get_latest_matching_file_name method not implemented")

    def get_latest_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        raise NotImplementedError("get_latest_matching_file_bytes method not implemented")

    def get_single_matching_file_name(self, filename_prefix: str) -> str:
        raise NotImplementedError("get_single_matching_file_name method not implemented")

    def get_single_matching_file_bytes(self, filename_prefix: str) -> BytesIO:
        raise NotImplementedError("get_single_matching_file_bytes method not implemented")

    def write_file(self, file_name: str, file_bytes: bytes) -> None:
        raise NotImplementedError("write_file method not implemented")

    def list_subfolders(self, folder_path: str) -> list[str]:
        raise NotImplementedError("list_subfolders method not implemented")
