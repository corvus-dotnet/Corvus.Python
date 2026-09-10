"""Copyright (c) Endjin Limited. All rights reserved."""

import os

from azure.appconfiguration import AzureAppConfigurationClient
from azure.identity import DefaultAzureCredential

from corvus_python.auth.audiences import TOKEN_AUDIENCE_SCOPES
from corvus_python.spark_utils.platform import FABRIC, SYNAPSE, get_platform
from corvus_python.spark_utils import get_spark_utils


class EnvironmentUtilities:
    """Platform-neutral access to tokens, secrets and configuration.

    Resolves identically on Synapse notebooks, Fabric notebooks, Azure Container Apps
    and local development. Subclass to add project-specific accessors.
    """

    token_audience_mapping = TOKEN_AUDIENCE_SCOPES

    # Naming conventions - override in a subclass where a project differs.
    environment_name_secret = "EnvironmentName"
    workspace_name_setting = "WorkspaceName"
    key_vault_linked_service = "KeyVault"
    key_vault_name_variable = "KeyVaultName"

    def __init__(self) -> None:
        self._platform: str = get_platform()
        self._spark_utils = None

    @property
    def platform(self) -> str:
        return self._platform

    @property
    def _uses_spark_utils(self) -> bool:
        """Fabric and Synapse both expose an mssparkutils-shaped API; ACA and local do not."""
        return self._platform in (FABRIC, SYNAPSE)

    def _get_spark_utils(self):
        if self._spark_utils is None:
            self._spark_utils = get_spark_utils()
        return self._spark_utils

    def get_environment_name(self) -> str:
        """Retrieves the environment name."""
        return self.get_secret(self.environment_name_secret)

    def get_token(self, audience: str) -> str:
        """Retrieves a token for the specified audience."""
        if self._uses_spark_utils:
            return self._get_spark_utils().credentials.getToken(audience)

        if audience not in self.token_audience_mapping:
            raise ValueError(f"Unsupported audience '{audience}'")
        return self._get_default_credential().get_token(self.token_audience_mapping[audience]).token

    def get_synapse_workspace_name(self) -> str:
        """Retrieves the Synapse workspace name.

        Named explicitly because the platforms disagree about what "workspace" means: only on
        Synapse does the notebook API report the workspace we want. Fabric reports its own
        workspace, so it reads the value from App Configuration like every other host does.
        """
        if self._platform == SYNAPSE:
            return self._get_spark_utils().env.getWorkspaceName()

        return self.get_app_config_setting(self.workspace_name_setting)

    def get_secret(self, secret_name: str) -> str:
        """Retrieves a secret from Azure Key Vault."""
        if self._uses_spark_utils:
            return self._get_spark_utils().credentials.getSecretWithLS(self.key_vault_linked_service, secret_name)

        from azure.keyvault.secrets import SecretClient

        key_vault_name = self._get_environment_variable(self.key_vault_name_variable)
        secret_client = SecretClient(
            vault_url=f"https://{key_vault_name}.vault.azure.net/",
            credential=self._get_default_credential(),
        )
        secret = secret_client.get_secret(secret_name)

        if secret.value is None:
            raise ValueError(f"Secret '{secret_name}' not found in Key Vault '{key_vault_name}'.")

        return secret.value

    def get_app_config_setting(self, setting_name: str) -> str:
        """Retrieves a setting from Azure App Configuration, labelled by environment name."""
        connection_string = self.get_secret("AppConfigurationReadOnlyConnectionString")

        if not isinstance(connection_string, str) or not connection_string:
            raise ValueError("AppConfigurationReadOnlyConnectionString is not set or is not a valid string.")

        client = AzureAppConfigurationClient.from_connection_string(connection_string)

        # Values are labelled with the environment name; requesting without the label returns nothing.
        setting = client.get_configuration_setting(setting_name, label=self.get_environment_name())

        if setting is None:
            raise ValueError(f"Setting '{setting_name}' not found in App Configuration.")

        return setting.value

    def _get_environment_variable(self, variable_name: str) -> str:
        value = os.environ.get(variable_name)
        if value is None:
            raise ValueError(f"Environment variable '{variable_name}' is not set.")
        return value

    def _get_default_credential(self) -> DefaultAzureCredential:
        return DefaultAzureCredential()
