import pytest

from corvus_python.storage.object_store_utils import build_object_store, read_file_bytes

ONELAKE_ROOT = "abfss://ws@onelake.dfs.fabric.microsoft.com/silver.Lakehouse/Files"


def test_read_file_bytes_reads_a_local_file(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"data")

    assert read_file_bytes(f"{tmp_path}/a.bin", None) == b"data"


def test_read_file_bytes_raises_file_not_found_for_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_file_bytes(f"{tmp_path}/missing.bin", None)


def test_read_file_bytes_raises_file_not_found_for_missing_folder_without_creating_it(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_file_bytes(f"{tmp_path}/nope/missing.bin", None)

    assert not (tmp_path / "nope").exists()


def test_build_object_store_passes_http_client_settings_as_client_options():
    """Polars accepts client settings in storage_options, but obstore panics if they are passed as store config."""
    store = build_object_store(
        ONELAKE_ROOT, {"bearer_token": "t", "allow_invalid_certificates": "true", "timeout": "30s"}
    )

    assert store.client_options == {"allow_invalid_certificates": "true", "timeout": "30s"}
    assert store.config["token"] == "t"
    assert "allow_invalid_certificates" not in store.config


def test_build_object_store_targets_onelake():
    store = build_object_store(ONELAKE_ROOT, {"bearer_token": "t"})

    assert store.config["account_name"] == "onelake"
    assert store.config["container_name"] == "ws"
    assert store.config["use_fabric_endpoint"] == "true"
    assert store.prefix == "silver.Lakehouse/Files"
