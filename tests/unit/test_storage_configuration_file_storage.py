import pytest
from io import BytesIO

from corvus_python.storage import (
    DataLakeLayer,
    FabricLakehouseFilesConfiguration,
    LocalFileSystemStorageConfiguration,
)
from corvus_python.storage.storage_configuration_file_storage import StorageConfigurationFileStorage


@pytest.fixture
def storage(tmp_path):
    """Files under <tmp>/bronze, plus a silver file, so the layer root is honoured."""
    bronze = tmp_path / "bronze"
    for name, content in {
        "raw/orders_2026-01-01.csv": b"a",
        "raw/orders_2026-01-02.csv": b"b",
        "raw/customers_2026-01-01.csv": b"c",
        "raw/archive/orders_2025-12-31.csv": b"old",
        "raw/2026/01/data.csv": b"nested",
    }.items():
        path = bronze / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (tmp_path / "silver").mkdir()
    (tmp_path / "silver" / "other.csv").write_bytes(b"not bronze")

    return StorageConfigurationFileStorage(LocalFileSystemStorageConfiguration(str(tmp_path)), DataLakeLayer.BRONZE)


def test_get_file_bytes(storage):
    result = storage.get_file_bytes("raw/orders_2026-01-01.csv")

    assert isinstance(result, BytesIO)
    assert result.read() == b"a"


def test_get_file_bytes_raises_file_not_found_for_missing_file(storage):
    with pytest.raises(FileNotFoundError):
        storage.get_file_bytes("raw/missing.csv")


def test_get_file_bytes_is_rooted_at_the_layer(storage):
    with pytest.raises(FileNotFoundError):
        storage.get_file_bytes("other.csv")


def test_get_matching_file_names_only_matches_files_in_the_prefix_folder(storage):
    assert sorted(storage.get_matching_file_names("raw/orders_")) == [
        "raw/orders_2026-01-01.csv",
        "raw/orders_2026-01-02.csv",
    ]


def test_get_matching_file_names_returns_empty_for_missing_folder(storage):
    assert storage.get_matching_file_names("nope/orders_") == []


def test_get_latest_matching_file_name(storage):
    assert storage.get_latest_matching_file_name("raw/orders_") == "raw/orders_2026-01-02.csv"


def test_get_latest_matching_file_bytes(storage):
    assert storage.get_latest_matching_file_bytes("raw/orders_").read() == b"b"


def test_get_latest_matching_file_name_raises_when_nothing_matches(storage):
    with pytest.raises(FileNotFoundError):
        storage.get_latest_matching_file_name("raw/invoices_")


def test_get_single_matching_file_name(storage):
    assert storage.get_single_matching_file_name("raw/customers_") == "raw/customers_2026-01-01.csv"


def test_get_single_matching_file_bytes(storage):
    assert storage.get_single_matching_file_bytes("raw/customers_").read() == b"c"


def test_get_single_matching_file_name_raises_when_multiple_match(storage):
    with pytest.raises(ValueError, match="Multiple files found"):
        storage.get_single_matching_file_name("raw/orders_")


def test_get_single_matching_file_name_raises_when_nothing_matches(storage):
    with pytest.raises(FileNotFoundError):
        storage.get_single_matching_file_name("raw/invoices_")


def test_write_file_creates_missing_folders(storage):
    storage.write_file("out/2026/report.csv", b"written")

    assert storage.get_file_bytes("out/2026/report.csv").read() == b"written"


def test_write_file_overwrites_existing_file(storage):
    storage.write_file("raw/orders_2026-01-01.csv", b"new")

    assert storage.get_file_bytes("raw/orders_2026-01-01.csv").read() == b"new"


def test_list_subfolders(storage):
    assert sorted(storage.list_subfolders("raw")) == ["2026", "archive"]


def test_list_subfolders_accepts_leading_and_trailing_separators(storage):
    assert sorted(storage.list_subfolders("/raw/")) == ["2026", "archive"]


def test_list_subfolders_at_layer_root(storage):
    assert storage.list_subfolders("") == ["raw"]


def test_list_subfolders_returns_empty_for_missing_folder(storage):
    assert storage.list_subfolders("nope") == []


def test_fabric_configuration_targets_onelake_with_the_configured_storage_options():
    """No request is made: this checks the store obstore builds from the Fabric URL and storage_options."""
    config = FabricLakehouseFilesConfiguration("ws", storage_options={"bearer_token": "t"})

    storage = StorageConfigurationFileStorage(config, DataLakeLayer.SILVER)

    assert storage._store.config["account_name"] == "onelake"
    assert storage._store.config["container_name"] == "ws"
    assert storage._store.config["use_fabric_endpoint"] == "true"
    assert storage._store.config["token"] == "t"
    assert storage._store.prefix == "silver.Lakehouse/Files"


def test_http_client_settings_in_storage_options_are_passed_as_client_options():
    """Polars accepts client settings in storage_options, but obstore panics if they are passed as store config."""
    config = FabricLakehouseFilesConfiguration(
        "ws", storage_options={"bearer_token": "t", "allow_invalid_certificates": "true", "timeout": "30s"}
    )

    storage = StorageConfigurationFileStorage(config, DataLakeLayer.SILVER)

    assert storage._store.client_options == {"allow_invalid_certificates": "true", "timeout": "30s"}
    assert storage._store.config["token"] == "t"
    assert "allow_invalid_certificates" not in storage._store.config
