import pytest

from corvus_python.platform import FABRIC, LOCAL, SYNAPSE
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

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"workspace_name": "b7cece7b-5f03-4129-b77e-cef7a41e9dc0"},
            {"workspace_name": "ws", "workspace_names": {"gold": "b7cece7b-5f03-4129-b77e-cef7a41e9dc0"}},
            {"workspace_name": "ws", "lakehouse_names": {"gold": "B7CECE7B-5F03-4129-B77E-CEF7A41E9DC0"}},
        ],
    )
    def test_rejects_ids_in_place_of_names(self, tls_calls, config_type, area, kwargs):
        """OneLake returns 400 Bad Request for paths that mix IDs and names."""
        with pytest.raises(ValueError, match="looks like an ID"):
            config_type(**kwargs)

    def test_passes_through_storage_options(self, tls_calls, config_type, area):
        options = {"bearer_token": "t"}

        config = config_type("ws", storage_options=options)

        assert config.storage_options == options


class TestAllowInvalidCertificates:
    @pytest.fixture
    def runtime(self, monkeypatch):
        def set_runtime(platform_name, spark):
            module = "corvus_python.storage.fabric_lakehouse_storage_configuration"
            monkeypatch.setattr(f"{module}.get_platform", lambda: platform_name)
            monkeypatch.setattr(f"{module}.is_spark_runtime", lambda: spark)

        return set_runtime

    def test_is_off_by_default(self, tls_calls, runtime):
        runtime(FABRIC, spark=True)

        config = FabricLakehouseTablesConfiguration("ws", storage_options={"bearer_token": "t"})

        assert config.storage_options == {"bearer_token": "t"}

    def test_adds_the_object_store_option_on_the_fabric_spark_runtime(self, tls_calls, runtime):
        """The Spark runtime proxies OneLake with a self-signed CA certificate that rustls rejects."""
        runtime(FABRIC, spark=True)
        options = {"bearer_token": "t"}

        config = FabricLakehouseTablesConfiguration("ws", storage_options=options, allow_invalid_certificates=True)

        assert config.storage_options == {"bearer_token": "t", "allow_invalid_certificates": "true"}
        assert options == {"bearer_token": "t"}, "the caller's dict must not be mutated"

    def test_works_without_other_storage_options(self, tls_calls, runtime):
        runtime(FABRIC, spark=True)

        config = FabricLakehouseFilesConfiguration("ws", allow_invalid_certificates=True)

        assert config.storage_options == {"allow_invalid_certificates": "true"}

    @pytest.mark.parametrize(
        "platform_name, spark",
        [(FABRIC, False), (LOCAL, False), (LOCAL, True), (SYNAPSE, True)],
        ids=["fabric-python-runtime", "local", "local-spark", "synapse-spark"],
    )
    def test_is_ignored_outside_the_fabric_spark_runtime(self, tls_calls, runtime, platform_name, spark):
        """The Fabric Python runtime reaches OneLake directly, so certificate validation stays on."""
        runtime(platform_name, spark)

        config = FabricLakehouseTablesConfiguration(
            "ws", storage_options={"bearer_token": "t"}, allow_invalid_certificates=True
        )

        assert config.storage_options == {"bearer_token": "t"}


def test_base_class_cannot_be_instantiated(tls_calls):
    """The base class has no Lakehouse area, so callers must choose Tables or Files."""
    with pytest.raises(TypeError):
        FabricLakehousePerLayerConfiguration("ws")
