import os
import pytest
from io import BytesIO
from corvus_python.storage.local_file_storage import LocalFileStorage


@pytest.fixture
def storage():
    """Fixture providing LocalFileStorage instance with test data directory."""
    test_data_path = os.path.join(os.path.dirname(__file__), "test_data", "local_file_storage")
    return LocalFileStorage(test_data_path)


# region Tests for get_file_bytes


def test_get_file_bytes_success(storage):
    """Test reading file bytes from a valid file."""
    result = storage.get_file_bytes("sample_logs/app_001.txt")

    assert isinstance(result, BytesIO)

    content = result.read()
    assert b"Log entry 001" in content
    assert b"2026-03-11 10:00:00" in content


def test_get_file_bytes_nested_file(storage):
    """Test reading file from nested directory."""
    result = storage.get_file_bytes("sample_logs/archive/app_old.txt")

    assert isinstance(result, BytesIO)
    content = result.read()
    assert b"Old log entry" in content


def test_get_file_bytes_file_not_found(storage):
    """Test that FileNotFoundError is raised for non-existent file."""
    with pytest.raises(FileNotFoundError):
        storage.get_file_bytes("sample_logs/nonexistent.txt")


# endregion

# region Tests for get_matching_file_names


def test_get_matching_file_names_exact_prefix(storage):
    """Test finding files matching exact prefix."""
    results = storage.get_matching_file_names("sample_logs/app_")

    assert len(results) == 3
    assert "sample_logs/app_001.txt" in results
    assert "sample_logs/app_002.txt" in results
    assert "sample_logs/app_003.txt" in results
    assert "sample_logs/system.txt" not in results


def test_get_matching_file_names_folder_scoped(storage):
    """Test that matching is scoped to the folder, not cross-folder."""
    results = storage.get_matching_file_names("sample_logs/app_")

    # Should not include files from archive subfolder
    assert "sample_logs/archive/app_old.txt" not in results
    assert len(results) == 3


def test_get_matching_file_names_single_match(storage):
    """Test matching with only one result."""
    results = storage.get_matching_file_names("sample_logs/system")

    assert len(results) == 1
    assert "sample_logs/system.txt" in results


def test_get_matching_file_names_no_matches(storage):
    """Test that empty list is returned when no files match."""
    results = storage.get_matching_file_names("sample_logs/nonexistent_")

    assert len(results) == 0
    assert isinstance(results, list)


def test_get_matching_file_names_csv_files(storage):
    """Test matching CSV files."""
    results = storage.get_matching_file_names("sample_data/data_")

    assert len(results) == 1
    assert "sample_data/data_001.csv" in results


# endregion

# region Tests for get_latest_matching_file_name


def test_get_latest_matching_file_name_success(storage):
    """Test getting the latest matching file."""
    result = storage.get_latest_matching_file_name("sample_logs/app_")

    # Should return the lexicographically last match
    assert result == "sample_logs/app_003.txt"


def test_get_latest_matching_file_name_single_file(storage):
    """Test getting latest when only one file matches."""
    result = storage.get_latest_matching_file_name("sample_data/data_")

    assert result == "sample_data/data_001.csv"


def test_get_latest_matching_file_name_no_matches(storage):
    """Test that FileNotFoundError is raised when no files match."""
    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_latest_matching_file_name("sample_logs/nonexistent_")

    assert "No files found" in str(exc_info.value)


# endregion

# region Tests for get_latest_matching_file_bytes


def test_get_latest_matching_file_bytes_success(storage):
    """Test getting bytes of the latest matching file."""
    result = storage.get_latest_matching_file_bytes("sample_logs/app_")

    assert isinstance(result, BytesIO)
    content = result.read()
    assert b"Log entry 003" in content
    assert b"2026-03-11 12:00:00" in content


def test_get_latest_matching_file_bytes_stream_position(storage):
    """Test that returned BytesIO is positioned at the start."""
    result = storage.get_latest_matching_file_bytes("sample_logs/app_")

    assert result.tell() == 0
    first_char = result.read(1)
    assert first_char == b"L"


# endregion

# region Tests for get_single_matching_file_name


def test_get_single_matching_file_name_success(storage):
    """Test getting single file when exactly one matches."""
    result = storage.get_single_matching_file_name("sample_data/data_")

    assert result == "sample_data/data_001.csv"


def test_get_single_matching_file_name_no_matches(storage):
    """Test that FileNotFoundError is raised when no files match."""
    with pytest.raises(FileNotFoundError) as exc_info:
        storage.get_single_matching_file_name("sample_logs/nonexistent_")

    assert "No files found" in str(exc_info.value)


def test_get_single_matching_file_name_multiple_matches(storage):
    """Test that ValueError is raised when multiple files match."""
    with pytest.raises(ValueError) as exc_info:
        storage.get_single_matching_file_name("sample_logs/app_")

    assert "Multiple files found" in str(exc_info.value)
    assert "expected only 1" in str(exc_info.value)


# endregion

# region Tests for get_single_matching_file_bytes


def test_get_single_matching_file_bytes_success(storage):
    """Test getting bytes of single matching file."""
    result = storage.get_single_matching_file_bytes("sample_data/data_")

    assert isinstance(result, BytesIO)
    content = result.read()
    assert b"col1,col2,col3" in content


def test_get_single_matching_file_bytes_multiple_matches(storage):
    """Test that ValueError is raised when multiple files match."""
    with pytest.raises(ValueError) as exc_info:
        storage.get_single_matching_file_bytes("sample_logs/app_")

    assert "Multiple files found" in str(exc_info.value)


# endregion

# region Tests for write_file


def test_write_file_new_file(storage):
    """Test writing a new file."""
    test_path = "sample_logs/test_write_new.txt"
    test_content = b"Test file content\nLine 2"

    storage.write_file(test_path, test_content)

    # Verify file was written
    full_path = os.path.join(storage._base_path, test_path)
    assert os.path.exists(full_path)

    with open(full_path, "rb") as f:
        written_content = f.read()

    assert written_content == test_content

    # Cleanup
    os.remove(full_path)


def test_write_file_overwrite_existing(storage):
    """Test overwriting an existing file."""
    test_path = "sample_logs/app_001.txt"
    new_content = b"Overwritten content"

    # Backup original
    original_bytes = storage.get_file_bytes(test_path)
    original_content = original_bytes.read()

    storage.write_file(test_path, new_content)

    # Verify overwrite
    result = storage.get_file_bytes(test_path)
    assert result.read() == new_content

    # Restore original
    storage.write_file(test_path, original_content)


def test_write_file_creates_nested_directories(storage):
    """Test that write_file creates necessary directories."""
    test_path = "sample_logs/new_subdir/test_file.txt"
    test_content = b"Content in nested directory"

    storage.write_file(test_path, test_content)

    # Verify file was written
    full_path = os.path.join(storage._base_path, test_path)
    assert os.path.exists(full_path)

    with open(full_path, "rb") as f:
        written_content = f.read()

    assert written_content == test_content

    # Cleanup
    os.remove(full_path)
    os.rmdir(os.path.dirname(full_path))


def test_write_file_empty_content(storage):
    """Test writing an empty file."""
    test_path = "sample_logs/test_empty.txt"
    test_content = b""

    storage.write_file(test_path, test_content)

    # Verify file was written
    full_path = os.path.join(storage._base_path, test_path)
    assert os.path.exists(full_path)
    assert os.path.getsize(full_path) == 0

    # Cleanup
    os.remove(full_path)


# endregion

# region Tests for list_subfolders


def test_list_subfolders_success(storage):
    """Test listing subfolders."""
    results = storage.list_subfolders("sample_logs")

    assert isinstance(results, list)
    assert "archive" in results


def test_list_subfolders_no_subfolders(storage):
    """Test listing subfolders when none exist."""
    results = storage.list_subfolders("sample_data")

    assert isinstance(results, list)
    assert len(results) == 0


def test_list_subfolders_nonexistent_folder(storage):
    """Test listing subfolders for non-existent folder."""
    results = storage.list_subfolders("nonexistent")

    assert isinstance(results, list)
    assert len(results) == 0


def test_list_subfolders_with_leading_slash(storage):
    """Test that leading slash in folder_path is handled."""
    results = storage.list_subfolders("/sample_logs")

    assert isinstance(results, list)
    assert "archive" in results


# endregion
