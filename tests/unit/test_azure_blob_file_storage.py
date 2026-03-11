import pytest
from io import BytesIO
from unittest.mock import patch, Mock, MagicMock
from azure.storage.blob import BlobPrefix
from corvus_python.storage.azure_blob_file_storage import AzureBlobFileStorage


@pytest.fixture
def mock_container_client():
    """Fixture providing a mocked ContainerClient."""
    return MagicMock()


@pytest.fixture
def storage(mock_container_client):
    """Fixture providing AzureBlobFileStorage instance with mocked container client."""
    return AzureBlobFileStorage(mock_container_client)


# region Tests for get_file_bytes


def test_get_file_bytes_success(storage, mock_container_client):
    """Test reading file bytes from a blob."""
    blob_name = "logs/app_001.log"
    test_content = b"Log entry 001\nStarted at 2026-03-11 10:00:00"

    mock_blob_client = MagicMock()
    mock_downloader = MagicMock()

    def readinto_side_effect(stream):
        stream.write(test_content)
        return len(test_content)

    mock_downloader.readinto.side_effect = readinto_side_effect
    mock_blob_client.download_blob.return_value = mock_downloader
    mock_container_client.get_blob_client.return_value = mock_blob_client

    result = storage.get_file_bytes(blob_name)

    assert isinstance(result, BytesIO)
    assert result.tell() == 0  # Stream should be at position 0
    content = result.read()
    assert content == test_content
    mock_container_client.get_blob_client.assert_called_once_with(blob_name)


def test_get_file_bytes_stream_position(storage, mock_container_client):
    """Test that returned BytesIO is positioned at the start."""
    blob_name = "data/file.csv"
    test_content = b"col1,col2,col3"

    mock_blob_client = MagicMock()
    mock_downloader = MagicMock()

    def readinto_side_effect(stream):
        stream.write(test_content)
        return len(test_content)

    mock_downloader.readinto.side_effect = readinto_side_effect
    mock_blob_client.download_blob.return_value = mock_downloader
    mock_container_client.get_blob_client.return_value = mock_blob_client

    result = storage.get_file_bytes(blob_name)

    assert result.tell() == 0
    first_char = result.read(1)
    assert first_char == b"c"


# endregion

# region Tests for get_matching_file_names


def test_get_matching_file_names_multiple_matches(storage, mock_container_client):
    """Test finding multiple blobs matching a prefix."""
    prefix = "logs/app_"
    matching_blobs = ["logs/app_001.log", "logs/app_002.log", "logs/app_003.log"]

    mock_container_client.list_blob_names.return_value = iter(matching_blobs)

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 3
    assert "logs/app_001.log" in results
    assert "logs/app_002.log" in results
    assert "logs/app_003.log" in results
    mock_container_client.list_blob_names.assert_called_once_with(name_starts_with=prefix)


def test_get_matching_file_names_folder_scoped(storage, mock_container_client):
    """Test that matching is scoped to the folder."""
    prefix = "logs/app_"
    # Include a blob from a subfolder to verify it's filtered out
    all_matching = ["logs/app_001.log", "logs/app_002.log", "logs/archive/app_old.log"]

    mock_container_client.list_blob_names.return_value = iter(all_matching)

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 2
    assert "logs/app_001.log" in results
    assert "logs/app_002.log" in results
    assert "logs/archive/app_old.log" not in results


def test_get_matching_file_names_no_matches(storage, mock_container_client):
    """Test that empty list is returned when no files match."""
    prefix = "logs/nonexistent_"

    mock_container_client.list_blob_names.return_value = iter([])

    results = storage.get_matching_file_names(prefix)

    assert len(results) == 0
    assert isinstance(results, list)


# endregion

# region Tests for get_latest_matching_file_name


def test_get_latest_matching_file_name_success(storage, mock_container_client):
    """Test getting the latest matching blob."""
    prefix = "logs/app_"
    matching_blobs = ["logs/app_001.log", "logs/app_003.log", "logs/app_002.log"]

    mock_container_client.list_blob_names.return_value = iter(matching_blobs)

    result = storage.get_latest_matching_file_name(prefix)

    assert result == "logs/app_003.log"


def test_get_latest_matching_file_name_no_matches(storage, mock_container_client):
    """Test that FileNotFoundError is raised when no files match."""
    prefix = "logs/nonexistent_"

    mock_container_client.list_blob_names.return_value = iter([])

    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_latest_matching_file_name(prefix)

    assert "No files found" in str(exc_info.value)


# endregion

# region Tests for get_latest_matching_file_bytes


def test_get_latest_matching_file_bytes_success(storage, mock_container_client):
    """Test getting bytes of the latest matching blob."""
    prefix = "logs/app_"
    matching_blobs = ["logs/app_001.log", "logs/app_003.log"]
    test_content = b"Log entry 003"

    # Mock list_blob_names
    mock_container_client.list_blob_names.return_value = iter(matching_blobs)

    # Mock get_blob_client and download
    mock_blob_client = MagicMock()
    mock_downloader = MagicMock()

    def readinto_side_effect(stream):
        stream.write(test_content)
        return len(test_content)

    mock_downloader.readinto.side_effect = readinto_side_effect
    mock_blob_client.download_blob.return_value = mock_downloader
    mock_container_client.get_blob_client.return_value = mock_blob_client

    result = storage.get_latest_matching_file_bytes(prefix)

    assert isinstance(result, BytesIO)
    content = result.read()
    assert content == test_content


# endregion

# region Tests for get_single_matching_file_name


def test_get_single_matching_file_name_success(storage, mock_container_client):
    """Test getting single blob when exactly one matches."""
    prefix = "data/data_"
    matching_blobs = ["data/data_001.csv"]

    mock_container_client.list_blob_names.return_value = iter(matching_blobs)

    result = storage.get_single_matching_file_name(prefix)

    assert result == "data/data_001.csv"


def test_get_single_matching_file_name_no_matches(storage, mock_container_client):
    """Test that FileNotFoundError is raised when no blobs match."""
    prefix = "logs/nonexistent_"

    mock_container_client.list_blob_names.return_value = iter([])

    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_single_matching_file_name(prefix)

    assert "No files found" in str(exc_info.value)


def test_get_single_matching_file_name_multiple_matches(storage, mock_container_client):
    """Test that ValueError is raised when multiple blobs match."""
    prefix = "logs/app_"
    matching_blobs = ["logs/app_001.log", "logs/app_002.log"]

    mock_container_client.list_blob_names.return_value = iter(matching_blobs)

    with pytest.raises(ValueError) as exc_info:
        storage.get_single_matching_file_name(prefix)

    assert "Multiple files found" in str(exc_info.value)
    assert "expected only 1" in str(exc_info.value)


# endregion

# region Tests for write_file


def test_write_file_success(storage, mock_container_client):
    """Test writing file content to a blob."""
    blob_name = "logs/app_004.log"
    file_content = b"New log entry"

    mock_blob_client = MagicMock()
    mock_container_client.get_blob_client.return_value = mock_blob_client

    storage.write_file(blob_name, file_content)

    mock_container_client.get_blob_client.assert_called_once_with(blob_name)
    mock_blob_client.upload_blob.assert_called_once_with(file_content, overwrite=True)


def test_write_file_empty_content(storage, mock_container_client):
    """Test writing empty content."""
    blob_name = "logs/empty.log"
    file_content = b""

    mock_blob_client = MagicMock()
    mock_container_client.get_blob_client.return_value = mock_blob_client

    storage.write_file(blob_name, file_content)

    mock_blob_client.upload_blob.assert_called_once_with(file_content, overwrite=True)


# endregion

# region Tests for list_subfolders


def test_list_subfolders_success(storage, mock_container_client):
    """Test listing virtual directories (folders)."""
    folder_path = "sample_logs"

    # Create mock BlobPrefix objects
    mock_prefix1 = MagicMock(spec=BlobPrefix)
    mock_prefix1.name = "sample_logs/archive/"

    mock_prefix2 = MagicMock(spec=BlobPrefix)
    mock_prefix2.name = "sample_logs/backups/"

    # Create mock BlobProperties (actual blobs, should be filtered)
    mock_blob = MagicMock()
    mock_blob.name = "sample_logs/system.txt"

    mock_container_client.walk_blobs.return_value = [mock_prefix1, mock_prefix2, mock_blob]

    results = storage.list_subfolders(folder_path)

    assert len(results) == 2
    assert "archive" in results
    assert "backups" in results
    mock_container_client.walk_blobs.assert_called_once_with(name_starts_with="sample_logs/", delimiter="/")


def test_list_subfolders_no_subfolders(storage, mock_container_client):
    """Test listing when no folders exist."""
    folder_path = "data"

    # Only return blobs, no BlobPrefix (folders)
    mock_blob = MagicMock()
    mock_blob.name = "data/data_001.csv"

    mock_container_client.walk_blobs.return_value = [mock_blob]

    results = storage.list_subfolders(folder_path)

    assert len(results) == 0


def test_list_subfolders_empty_folder(storage, mock_container_client):
    """Test listing subfolders from empty folder."""
    folder_path = "empty"

    mock_container_client.walk_blobs.return_value = []

    results = storage.list_subfolders(folder_path)

    assert len(results) == 0


def test_list_subfolders_path_formatting(storage, mock_container_client):
    """Test that folder path is properly formatted with trailing slash."""
    folder_path = "logs/archive"

    mock_prefix = MagicMock(spec=BlobPrefix)
    mock_prefix.name = "logs/archive/old/"

    mock_container_client.walk_blobs.return_value = [mock_prefix]

    storage.list_subfolders(folder_path)

    # Should add trailing slash
    mock_container_client.walk_blobs.assert_called_once_with(name_starts_with="logs/archive/", delimiter="/")


# endregion
