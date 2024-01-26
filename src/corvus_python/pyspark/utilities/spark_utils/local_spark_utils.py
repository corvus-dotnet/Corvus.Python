"""Copyright (c) Endjin Limited. All rights reserved."""

from azure.identity import AzureCliCredential, CredentialUnavailableError


class LSRLinkedServiceFailure(Exception):
    """Exception raised when the Linked Service can't be found.

    :param linked_service: The name of the linked service
    :type linked_service: str
    """

    def __init__(self, linked_service: str):
        """Constructor method
        """
        self.linked_service = linked_service
        self.message = f"""
        Could not find Linked Service '{linked_service}'; the linked service does not exist or is not published.
        """
        super().__init__(self.message)


class SecretNotFound(Exception):
    """Exception raised when a Key Vault Secret can't be found.

    :param secret_name: The name of the secret
    :type secret_name: str
    """

    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        self.message = f"""
        A secret with (name/id) '{secret_name}' was not found in this key vault.
        """
        super().__init__(self.message)


class LocalCredentialUtils():
    """Class which mirrors elements of the mssparkutils.credentials API. Intentionally not a full representation -
    additional methods will be added to it as and when the need arises.

    :param config: Dictionary representing configuration required for `credentials` API. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    :type config: dict
    """
    def __init__(self, config: dict):
        self.config = config

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
                raise ValueError(
                    f"Unknown secret type {target_secret.get('type')}")

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

        credential = AzureCliCredential()

        try:
            token = credential.get_token(scope, tenant_id=tenant_id)
        except CredentialUnavailableError:
            raise RuntimeError("Please login to the Azure CLI using `az login --tenant <tenant> "
                               "--use-device-code` to authenticate.")

        return token


class LocalEnvUtils():
    """Class which mirrors elements of the mssparkutils.env API. Intentionally not a full representation - additional
    methods will be added to it as and when the need arises.

    :param config: Dictionary representing configuration required for `env` API. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    :type config: dict
    """
    def __init__(self, config: dict):
        self.config = config

    def getWorkspaceName(self) -> str:
        return self.config.get("workspaceName")


class LocalSparkUtils():
    """Class which mirrors elements of the mssparkutils API. Intentionally not a full representation - additional
    sub-classes will be added to it as and when the need arises.

    :param local_config: Dictionary representing full `LocalSparkUtils` configuration. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    :type local_config: dict
    """
    def __init__(self, local_config: dict):
        self.config = local_config
        self.credentials = LocalCredentialUtils(local_config.get("credentials"))
        self.env = LocalEnvUtils(local_config.get("env"))
