import pytest

from corvus_python.storage.azure_data_lake_storage_configuration import (
    AzureDataLakeFileSystemPerLayerConfiguration,
    AzureDataLakeSingleFileSystemConfiguration,
)
from corvus_python.storage.storage_configuration import DataLakeLayer


@pytest.fixture
def tls_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "corvus_python.storage.azure_data_lake_storage_configuration.configure_tls_trust_store",
        lambda: calls.append(True),
    )
    return calls


class TestAzureDataLakeFileSystemPerLayerConfiguration:
    def test_configures_tls_trust_store_on_construction(self, tls_calls):
        """object_store builds its TLS config once per process, so this must precede any read."""
        AzureDataLakeFileSystemPerLayerConfiguration("acct")

        assert tls_calls == [True]

    def test_builds_a_file_system_per_layer_path(self, tls_calls):
        config = AzureDataLakeFileSystemPerLayerConfiguration("acct")

        assert config.get_full_path(DataLakeLayer.SILVER, "db/table") == "abfss://silver@acct.dfs.core.windows.net/db/table"


class TestAzureDataLakeSingleFileSystemConfiguration:
    def test_configures_tls_trust_store_on_construction(self, tls_calls):
        AzureDataLakeSingleFileSystemConfiguration("acct", "datalake")

        assert tls_calls == [True]

    def test_builds_a_single_file_system_path(self, tls_calls):
        config = AzureDataLakeSingleFileSystemConfiguration("acct", "datalake")

        assert (
            config.get_full_path(DataLakeLayer.GOLD, "db/table")
            == "abfss://datalake@acct.dfs.core.windows.net/gold/db/table"
        )
