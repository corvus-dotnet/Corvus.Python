import pytest

from corvus_python.storage import (
    FabricLakehouseFilesConfiguration,
    FabricLakehousePerLayerConfiguration,
    FabricLakehouseTablesConfiguration,
)
from corvus_python.storage.storage_configuration import DataLakeLayer


@pytest.fixture
def tls_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "corvus_python.storage.fabric_lakehouse_storage_configuration.configure_tls_trust_store",
        lambda: calls.append(True),
    )
    return calls


@pytest.mark.parametrize(
    "config_type, area",
    [(FabricLakehouseTablesConfiguration, "Tables"), (FabricLakehouseFilesConfiguration, "Files")],
)
class TestFabricLakehouseConfigurations:
    def test_configures_tls_trust_store_on_construction(self, tls_calls, config_type, area):
        """object_store builds its TLS config once per process, so this must precede any read."""
        config_type("ws")

        assert tls_calls == [True]

    @pytest.mark.parametrize("layer", list(DataLakeLayer))
    def test_defaults_to_a_lakehouse_named_after_each_layer(self, tls_calls, config_type, area, layer):
        config = config_type("ws")

        assert (
            config.get_full_path(layer, "db/table")
            == f"abfss://ws@onelake.dfs.fabric.microsoft.com/{layer}.Lakehouse/{area}/db/table"
        )

    def test_strips_leading_separator_from_path(self, tls_calls, config_type, area):
        """PolarsDeltaTableRepository builds '{base_path}/...', so an empty base_path yields a leading '/'."""
        config = config_type("ws")

        assert (
            config.get_full_path(DataLakeLayer.SILVER, "/db/table")
            == f"abfss://ws@onelake.dfs.fabric.microsoft.com/silver.Lakehouse/{area}/db/table"
        )

    def test_uses_lakehouse_name_overrides(self, tls_calls, config_type, area):
        config = config_type("ws", lakehouse_names={DataLakeLayer.SILVER: "lh_silver"})

        assert (
            config.get_full_path(DataLakeLayer.SILVER, "x")
            == f"abfss://ws@onelake.dfs.fabric.microsoft.com/lh_silver.Lakehouse/{area}/x"
        )
        assert (
            config.get_full_path(DataLakeLayer.GOLD, "x")
            == f"abfss://ws@onelake.dfs.fabric.microsoft.com/gold.Lakehouse/{area}/x"
        )

    def test_uses_workspace_name_overrides(self, tls_calls, config_type, area):
        config = config_type("ws", workspace_names={DataLakeLayer.GOLD: "ws_gold"})

        assert (
            config.get_full_path(DataLakeLayer.GOLD, "x")
            == f"abfss://ws_gold@onelake.dfs.fabric.microsoft.com/gold.Lakehouse/{area}/x"
        )
        assert (
            config.get_full_path(DataLakeLayer.BRONZE, "x")
            == f"abfss://ws@onelake.dfs.fabric.microsoft.com/bronze.Lakehouse/{area}/x"
        )

    def test_accepts_string_layer_keys(self, tls_calls, config_type, area):
        config = config_type("ws", lakehouse_names={"bronze": "lh_bronze"}, workspace_names={"bronze": "ws_bronze"})

        assert (
            config.get_full_path(DataLakeLayer.BRONZE, "x")
            == f"abfss://ws_bronze@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/{area}/x"
        )

    def test_rejects_unknown_layer_in_overrides(self, tls_calls, config_type, area):
        with pytest.raises(ValueError):
            config_type("ws", lakehouse_names={"platinum": "lh"})

    def test_passes_through_storage_options(self, tls_calls, config_type, area):
        options = {"bearer_token": "t"}

        config = config_type("ws", storage_options=options)

        assert config.storage_options == options


def test_base_class_cannot_be_instantiated(tls_calls):
    """The base class has no Lakehouse area, so callers must choose Tables or Files."""
    with pytest.raises(TypeError):
        FabricLakehousePerLayerConfiguration("ws")
