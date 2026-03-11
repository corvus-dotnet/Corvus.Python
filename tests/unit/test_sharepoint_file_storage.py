import pytest
import requests
from io import BytesIO
from unittest.mock import patch, Mock, MagicMock
from corvus_python.storage.sharepoint_file_storage import SharePointFileStorage
from corvus_python.sharepoint import SharePointUtilities


@pytest.fixture
def mock_sharepoint_utilities():
    """Fixture for patching SharePointUtilities."""
    with patch("corvus_python.storage.sharepoint_file_storage.SharePointUtilities") as mock_util:
        yield mock_util


@pytest.fixture
def storage(mock_sharepoint_utilities):
    """Fixture providing SharePointFileStorage instance with mocked utilities."""
    mock_sharepoint_utilities.get_drive_id.return_value = "fake_drive_id"

    return SharePointFileStorage(
        sharepoint_tenant_fqdn="tenant.sharepoint.com",
        sharepoint_site_name="TestSite",
        library_name="Documents",
        auth_token="fake_token",
    )


# region Tests for get_file_bytes


def test_get_file_bytes_success(storage, mock_sharepoint_utilities):
    """Test reading file bytes from SharePoint."""
    filename = "logs/app_001.log"
    test_content = b"Log entry 001"
    download_url = "https://example.sharepoint.com/download/file"

    mock_sharepoint_utilities.get_file_download_url.return_value = download_url

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.content = test_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = storage.get_file_bytes(filename)

        assert isinstance(result, BytesIO)
        assert result.tell() == 0  # Stream should be at position 0
        content = result.read()
        assert content == test_content
        mock_get.assert_called_once_with(download_url)


def test_get_file_bytes_network_error(storage, mock_sharepoint_utilities):
    """Test handling of HTTP errors."""
    filename = "logs/app_001.log"
    download_url = "https://example.sharepoint.com/download/file"

    mock_sharepoint_utilities.get_file_download_url.return_value = download_url

    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

        with pytest.raises(requests.exceptions.HTTPError):
            storage.get_file_bytes(filename)


# endregion

# region Tests for get_matching_file_names


def test_get_matching_file_names_multiple_matches(storage, mock_sharepoint_utilities):
    """Test finding multiple files matching a prefix."""
    prefix = "sample_logs/app_"
    mock_files = [
        {"name": "app_001.txt", "file": {}},
        {"name": "app_002.txt", "file": {}},
        {"name": "app_003.txt", "file": {}},
    ]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 3
    assert "sample_logs/app_001.txt" in results
    assert "sample_logs/app_002.txt" in results
    assert "sample_logs/app_003.txt" in results
    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.assert_called_once_with(
        drive_id="fake_drive_id", folder_name="sample_logs", token="fake_token"
    )


def test_get_matching_file_names_folder_scoped(storage, mock_sharepoint_utilities):
    """Test that matching is scoped to the folder."""
    prefix = "logs/app_"
    mock_files = [
        {"name": "app_001.log", "file": {}},
        {"name": "app_002.log", "file": {}},
        {"name": "system.log", "file": {}},  # Should be filtered
    ]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 2
    assert "logs/app_001.log" in results
    assert "logs/app_002.log" in results


def test_get_matching_file_names_no_matches(storage, mock_sharepoint_utilities):
    """Test that empty list is returned when no files match."""
    prefix = "logs/nonexistent_"

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = []

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 0


def test_get_matching_file_names_filters_non_files(storage, mock_sharepoint_utilities):
    """Test that only files (not folders) are returned."""
    prefix = "logs/app_"
    mock_files = [
        {"name": "app_001.log", "file": {}},
        {"name": "subfolder", "folder": {}},  # Should be filtered (has folder, no file)
        {"name": "app_002.log", "file": {}},
    ]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    results = storage.get_matching_file_names(prefix)

    # The implementation filters for files with "file" key in the dict
    assert len(results) == 2


# endregion

# region Tests for get_latest_matching_file_name


def test_get_latest_matching_file_name_success(storage, mock_sharepoint_utilities):
    """Test getting the latest matching file."""
    prefix = "logs/app_"
    mock_files = [
        {"name": "app_001.log", "file": {}},
        {"name": "app_003.log", "file": {}},
        {"name": "app_002.log", "file": {}},
    ]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    result = storage.get_latest_matching_file_name(prefix)

    assert result == "logs/app_003.log"


def test_get_latest_matching_file_name_no_matches(storage, mock_sharepoint_utilities):
    """Test that FileNotFoundError is raised when no files match."""
    prefix = "logs/nonexistent_"

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = []

    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_latest_matching_file_name(prefix)

    assert "No files found" in str(exc_info.value)


# endregion

# region Tests for get_latest_matching_file_bytes


def test_get_latest_matching_file_bytes_success(storage, mock_sharepoint_utilities):
    """Test getting bytes of the latest matching file."""
    prefix = "logs/app_"
    mock_files = [
        {"name": "app_001.log", "file": {}},
        {"name": "app_003.log", "file": {}},
    ]
    test_content = b"Latest log entry"
    download_url = "https://example.sharepoint.com/download/latest"

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files
    mock_sharepoint_utilities.get_file_download_url.return_value = download_url

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.content = test_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = storage.get_latest_matching_file_bytes(prefix)

        assert isinstance(result, BytesIO)
        content = result.read()
        assert content == test_content


# endregion

# region Tests for get_single_matching_file_name


def test_get_single_matching_file_name_success(storage, mock_sharepoint_utilities):
    """Test getting single file when exactly one matches."""
    prefix = "data/data_"
    mock_files = [{"name": "data_001.csv", "file": {}}]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    result = storage.get_single_matching_file_name(prefix)

    assert result == "data/data_001.csv"


def test_get_single_matching_file_name_no_matches(storage, mock_sharepoint_utilities):
    """Test that FileNotFoundError is raised when no files match."""
    prefix = "logs/nonexistent_"

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = []

    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_single_matching_file_name(prefix)

    assert "No files found" in str(exc_info.value)


def test_get_single_matching_file_name_multiple_matches(storage, mock_sharepoint_utilities):
    """Test that ValueError is raised when multiple files match."""
    prefix = "logs/app_"
    mock_files = [
        {"name": "app_001.log", "file": {}},
        {"name": "app_002.log", "file": {}},
    ]

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files

    with pytest.raises(ValueError) as exc_info:
        storage.get_single_matching_file_name(prefix)

    assert "Multiple files found" in str(exc_info.value)
    assert "expected only 1" in str(exc_info.value)


# endregion

# region Tests for get_single_matching_file_bytes


def test_get_single_matching_file_bytes_success(storage, mock_sharepoint_utilities):
    """Test getting bytes of single matching file."""
    prefix = "data/data_"
    mock_files = [{"name": "data_001.csv", "file": {}}]
    test_content = b"col1,col2,col3"
    download_url = "https://example.sharepoint.com/download/data"

    mock_sharepoint_utilities.get_download_urls_for_files_in_folder.return_value = mock_files
    mock_sharepoint_utilities.get_file_download_url.return_value = download_url

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.content = test_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = storage.get_single_matching_file_bytes(prefix)

        assert isinstance(result, BytesIO)
        content = result.read()
        assert content == test_content


# endregion

# region Tests for write_file


def test_write_file_success(storage, mock_sharepoint_utilities):
    """Test writing file to SharePoint."""
    filename = "logs/app_004.log"
    file_content = b"New log entry"

    mock_sharepoint_utilities.save_file.return_value = {"id": "file123", "webUrl": "https://..."}

    storage.write_file(filename, file_content)

    mock_sharepoint_utilities.save_file.assert_called_once_with(
        "fake_drive_id", filename, "fake_token", bytearray(file_content)
    )


def test_write_file_empty_content(storage, mock_sharepoint_utilities):
    """Test writing empty content."""
    filename = "logs/empty.log"
    file_content = b""

    mock_sharepoint_utilities.save_file.return_value = {"id": "file_empty", "webUrl": "https://..."}

    storage.write_file(filename, file_content)

    mock_sharepoint_utilities.save_file.assert_called_once()


# endregion

# region Tests for list_subfolders


def test_list_subfolders_success(storage, mock_sharepoint_utilities):
    """Test listing subfolders."""
    folder_path = "sample_logs"
    mock_items = [
        {"webUrl": "https://example.sharepoint.com/folders/archive", "folder": {}, "name": "archive"},
        {"webUrl": "https://example.sharepoint.com/folders/backups", "folder": {}, "name": "backups"},
    ]

    mock_sharepoint_utilities.get_items_in_folder.return_value = mock_items

    results = storage.list_subfolders(folder_path)

    assert len(results) == 2
    assert "archive" in results
    assert "backups" in results
    mock_sharepoint_utilities.get_items_in_folder.assert_called_once_with(
        drive_id="fake_drive_id", folder_path=folder_path, token="fake_token"
    )


def test_list_subfolders_nested_path(storage, mock_sharepoint_utilities):
    """Test listing subfolders in a nested path."""
    folder_path = "logs/archive"
    mock_items = [
        {"webUrl": "https://example.sharepoint.com/folders/2024", "folder": {}, "name": "2024"},
        {"webUrl": "https://example.sharepoint.com/folders/2025", "folder": {}, "name": "2025"},
    ]

    mock_sharepoint_utilities.get_items_in_folder.return_value = mock_items

    results = storage.list_subfolders(folder_path)

    assert len(results) == 2
    mock_sharepoint_utilities.get_items_in_folder.assert_called_once_with(
        drive_id="fake_drive_id", folder_path=folder_path, token="fake_token"
    )


def test_list_subfolders_no_subfolders(storage, mock_sharepoint_utilities):
    """Test listing when no subfolders exist."""
    folder_path = "data"

    mock_sharepoint_utilities.get_items_in_folder.return_value = []

    results = storage.list_subfolders(folder_path)

    assert len(results) == 0


# endregion
