"""Copyright (c) Endjin Limited. All rights reserved."""


class LSRLinkedServiceFailure(Exception):
    """Exception raised when the Linked Service can't be found.

    Attributes:
        linked_service -- the name of the linked service
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
        secret_name -- the name of the secret
    """

    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        self.message = f"""
        A secret with (name/id) '{secret_name}' was not found in this key vault.
        """
        super().__init__(self.message)


class LocalCredentialUtils():
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


class LocalSparkUtils():
    def __init__(self, local_config: dict):
        self.config = local_config
        self.credentials = LocalCredentialUtils(local_config.get("credentials"))
