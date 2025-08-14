"""Copyright (c) Endjin Limited. All rights reserved."""

from typing import Dict, TypedDict, Literal, Optional
from corvus_python.auth import get_az_cli_token


class StaticSecretConfig(TypedDict):
    """Configuration for a static secret."""

    type: Literal["static"]
    value: str


class LinkedServiceSecretsConfig(TypedDict, total=False):
    """Configuration for secrets within a linked service.

    Keys are secret names, values are secret configurations.
    """

    pass  # This allows any string key with StaticSecretConfig values


class GetSecretWithLSConfig(TypedDict, total=False):
    """Configuration for getSecretWithLS method.

    Keys are linked service names, values are their secret configurations.
    """

    pass  # This allows any string key with LinkedServiceSecretsConfig values


class GetTokenConfig(TypedDict):
    """Configuration for getToken method."""

    tenantId: str


class CredentialsConfig(TypedDict):
    """Configuration for credentials utilities."""

    getSecretWithLS: Dict[str, Dict[str, StaticSecretConfig]]
    getToken: Optional[GetTokenConfig]


class EnvConfig(TypedDict):
    """Configuration for environment utilities."""

    getWorkspaceName: str


class LocalSparkUtilsConfig(TypedDict):
    """Main configuration for LocalSparkUtils."""

    credentials: CredentialsConfig
    env: EnvConfig


class LSRLinkedServiceFailure(Exception):
    """Exception raised when the Linked Service can't be found.

    Attributes:
        linked_service (str): The name of the linked service.
        message (str): The error message explaining the failure.
    """

    def __init__(self, linked_service: str):
        """Constructor method.

        Args:
            linked_service (str): The name of the linked service.
        """
        self.linked_service = linked_service
        self.message = f"""
        Could not find Linked Service '{linked_service}'; the linked service does not exist or is not published.
        """
        super().__init__(self.message)


class SecretNotFound(Exception):
    """Exception raised when a Key Vault Secret can't be found.

    Attributes:
        secret_name (str): The name of the secret.
        message (str): The error message explaining the failure.
    """

    def __init__(self, secret_name: str):
        """Constructor method.

        Args:
            secret_name (str): The name of the secret.
        """

        self.secret_name = secret_name
        self.message = f"""
        A secret with (name/id) '{secret_name}' was not found in this key vault.
        """
        super().__init__(self.message)


class LocalCredentialUtils:
    """Class which mirrors elements of the mssparkutils.credentials API. Intentionally not a full representation -
    additional methods will be added to it as and when the need arises.

    Attributes:
        config (CredentialsConfig): Configuration required for `credentials` API. See
            https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    """

    def __init__(self, config: CredentialsConfig):
        """Constructor method

        Args:
            config (CredentialsConfig): Configuration required for `credentials` API. See
                https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
        """
        self.config: CredentialsConfig = config

    def getSecretWithLS(self, linked_service: str, secret_name: str) -> str:
        lookup = self.config.get("getSecretWithLS")

        target_ls = lookup.get(linked_service)

        if not target_ls:
            raise LSRLinkedServiceFailure(linked_service)

        target_secret = target_ls.get(secret_name)

        if not target_secret:
            raise SecretNotFound(secret_name)

        match target_secret.get("type"):
            case "static":
                return target_secret.get("value")
            case _:
                raise ValueError(f"Unknown secret type {target_secret.get('type')}")

    def getToken(self, audience: str) -> str:
        scopes = {
            "Storage": "https://storage.azure.com/.default",
            "Vault": "https://vault.azure.net/.default",
            "AzureManagement": "https://management.azure.com/.default",
            "DW": "https://database.windows.net/.default",
            "Synapse": "https://dev.azuresynapse.net/.default",
            "DataLakeStore": "https://datalake.azure.net/.default",
            "DF": "https://datafactory.azure.net/.default",
            "AzureDataExplorer": "https://kusto.kusto.windows.net/.default",
            "AzureOSSDB": "https://ossrdbms-aad.database.windows.net/.default",
        }

        scope = scopes.get(audience)

        if not scope:
            raise ValueError(f"Unsupported audience '{audience}'")

        tenant_id = None
        get_token_config = self.config.get("getToken")
        if get_token_config:
            tenant_id = get_token_config.get("tenantId")

        return get_az_cli_token(scope, tenant_id=tenant_id)  # type: ignore


class LocalEnvUtils:
    """Class which mirrors elements of the mssparkutils.env API. Intentionally not a full representation - additional
    methods will be added to it as and when the need arises.

    Attributes:
        config (EnvConfig): Configuration required for `env` API. See
            https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.

    """

    def __init__(self, config: EnvConfig):
        """Constructor method

        Args:
            config (EnvConfig): Configuration required for `env` API. See
                https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
        """

        self.config: EnvConfig = config

    def getWorkspaceName(self) -> str:
        return self.config.get("getWorkspaceName")


class LocalSparkUtils:
    """Class which mirrors elements of the mssparkutils API. Intentionally not a full representation - additional
    sub-classes will be added to it as and when the need arises.

    Attributes:
        config (LocalSparkUtilsConfig): Full `LocalSparkUtils` configuration. See
            https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
        credentials (LocalCredentialUtils): LocalCredentialUtils instance.
        env (LocalEnvUtils): LocalEnvUtils instance.
    """

    def __init__(self, local_config: LocalSparkUtilsConfig):
        """Constructor method

        Args:
            local_config (LocalSparkUtilsConfig): Full `LocalSparkUtils` configuration. See
                https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
        """
        self.config: LocalSparkUtilsConfig = local_config
        self.credentials = LocalCredentialUtils(local_config.get("credentials"))
        self.env = LocalEnvUtils(local_config.get("env"))
