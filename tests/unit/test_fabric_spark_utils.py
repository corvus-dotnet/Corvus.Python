import pytest

from corvus_python.auth.audiences import TOKEN_AUDIENCE_SCOPES
from corvus_python.spark_utils.fabric_spark_utils import (
    FabricSparkUtils,
    FabricSparkUtilsConfig,
)


@pytest.fixture(autouse=True)
def no_variable_library(monkeypatch):
    """Fabric's variable library is unavailable off-platform; force the env var fallback."""
    monkeypatch.setattr(
        "corvus_python.spark_utils.fabric_spark_utils._get_variable_library_value",
        lambda name, library_name: None,
    )


def _library_returning(monkeypatch, value, calls=None):
    def fake(name, library_name):
        if calls is not None:
            calls.append((name, library_name))
        return value

    monkeypatch.setattr("corvus_python.spark_utils.fabric_spark_utils._get_variable_library_value", fake)


class TestFabricSparkUtilsConfig:
    def test_resolves_from_environment_variables(self, monkeypatch):
        monkeypatch.setenv("KeyVaultName", "kv-from-env")

        config = FabricSparkUtilsConfig.resolve()

        assert config.key_vault_name == "kv-from-env"

    def test_prefers_the_named_variable_library_over_the_environment(self, monkeypatch):
        calls = []
        monkeypatch.setenv("KeyVaultName", "kv-from-env")
        _library_returning(monkeypatch, "kv-from-library", calls)

        config = FabricSparkUtilsConfig.resolve("library_name")

        assert config.key_vault_name == "kv-from-library"
        assert calls == [("KeyVaultName", "library_name")]

    def test_skips_the_variable_library_when_no_name_is_given(self, monkeypatch):
        """corvus cannot know what a project calls its library, so it never guesses one."""
        calls = []
        monkeypatch.setenv("KeyVaultName", "kv-from-env")
        _library_returning(monkeypatch, "kv-from-library", calls)

        config = FabricSparkUtilsConfig.resolve()

        assert config.key_vault_name == "kv-from-env"
        assert calls == []

    def test_falls_back_to_the_environment_when_the_library_lacks_the_value(self, monkeypatch):
        monkeypatch.setenv("KeyVaultName", "kv-from-env")
        _library_returning(monkeypatch, None)

        config = FabricSparkUtilsConfig.resolve("library_name")

        assert config.key_vault_name == "kv-from-env"

    def test_missing_values_resolve_to_none_rather_than_raising(self, monkeypatch):
        """Token acquisition needs no configuration, so resolution must not fail here."""
        monkeypatch.delenv("KeyVaultName", raising=False)

        config = FabricSparkUtilsConfig.resolve("library_name")

        assert config.key_vault_name is None


class TestFabricSparkUtils:
    def test_get_secret_with_ls_raises_when_key_vault_unconfigured(self, monkeypatch):
        monkeypatch.delenv("KeyVaultName", raising=False)

        utils = FabricSparkUtils()

        with pytest.raises(ValueError, match="key_vault_name is not configured"):
            utils.credentials.getSecretWithLS("KeyVault", "AnySecret")

    def test_get_secret_with_ls_error_names_the_library_it_searched(self, monkeypatch):
        monkeypatch.delenv("KeyVaultName", raising=False)

        utils = FabricSparkUtils("library_name")

        with pytest.raises(ValueError, match="variable library 'library_name'"):
            utils.credentials.getSecretWithLS("KeyVault", "AnySecret")

    def test_env_workspace_name_returns_the_fabric_workspace(self, monkeypatch):
        """env.getWorkspaceName mirrors notebookutils and reports the *Fabric* workspace.

        That is not the Synapse workspace: callers building a Synapse endpoint must read that
        name from configuration, or they produce an invalid endpoint.
        """
        import notebookutils

        monkeypatch.setattr(notebookutils.runtime, "context", {"currentWorkspaceName": "[DEV] Data Prep"})

        utils = FabricSparkUtils()

        assert utils.env.getWorkspaceName() == "[DEV] Data Prep"

    def test_get_token_maps_aliases_to_full_resource_scopes(self, monkeypatch):
        """Fabric rejects the short keyword forms but accepts full resource scopes."""
        requested = []

        class FakeCredentials:
            @staticmethod
            def getToken(audience):
                requested.append(audience)
                return "token"

        monkeypatch.setattr("notebookutils.credentials", FakeCredentials)

        utils = FabricSparkUtils()
        utils.credentials.getToken("Synapse")
        utils.credentials.getToken("https://custom.example.com/.default")

        assert requested == [
            TOKEN_AUDIENCE_SCOPES["Synapse"],
            "https://custom.example.com/.default",
        ]
