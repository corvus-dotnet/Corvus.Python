"""Copyright (c) Endjin Limited. All rights reserved."""


class LSRLinkedServiceFailure(Exception):
    """Exception raised when the Linked Service can't be found.

    Attributes:
        linked_service (str): the name of the linked service
    """

    def __init__(self, linked_service: str):
        self.linked_service = linked_service
        self.message = f"""
        Could not find Linked Service '{linked_service}'; the linked service does not exist or is not published.
        """
        super().__init__(self.message)


class SecretNotFound(Exception):
    """Exception raised when a Key Vault Secret can't be found.

    Attributes:
        secret_name (str): the name of the secret
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

    Attributes:
        config (dict): Dictionary representing configuration required for `credentials` API. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
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


class LocalEnvUtils():
    """Class which mirrors elements of the mssparkutils.env API. Intentionally not a full representation - additional
    methods will be added to it as and when the need arises.

    Attributes:
        config (dict): Dictionary representing configuration required for `env` API. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    """
    def __init__(self, config: dict):
        self.config = config

    def getWorkspaceName(self) -> str:
        return self.config.get("workspaceName")


class LocalSparkUtils():
    """Class which mirrors elements of the mssparkutils API. Intentionally not a full representation - additional
    sub-classes will be added to it as and when the need arises.

    Attributes:
        local_config (dict): Dictionary representing full `LocalSparkUtils` configuration. See
        https://github.com/corvus-dotnet/Corvus.Python/blob/main/README.md for details.
    """
    def __init__(self, local_config: dict):
        self.config = local_config
        self.credentials = LocalCredentialUtils(local_config.get("credentials"))
        self.env = LocalEnvUtils(local_config.get("env"))
