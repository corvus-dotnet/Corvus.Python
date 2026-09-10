"""Copyright (c) Endjin Limited. All rights reserved."""

import base64
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

from corvus_python.auth.audiences import TOKEN_AUDIENCE_SCOPES


class FabricTokenCredential:
    def get_token(self, *scopes, **kwargs):
        from azure.core.credentials import AccessToken
        import notebookutils

        # kwargs (claims, tenant_id, enable_cae) are intentionally ignored: notebookutils
        # issues tokens for the executing identity only and cannot satisfy a CAE challenge.
        token = notebookutils.credentials.getToken(scopes[0])
        return AccessToken(token, _token_expiry(token))


def _token_expiry(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return int(time.time()) + 300


def _get_variable_library_value(name: str, library_name: Optional[str] = None) -> Optional[str]:
    """VERIFY THIS CALL SHAPE - the notebookutils.variableLibrary API is unconfirmed.
    This is the only function that should need adjusting."""
    try:
        import notebookutils

        vl = getattr(notebookutils, "variableLibrary", None)
        if vl is None:
            return None
        if library_name:
            return vl.get(f"$(/**/{library_name}/{name})")
        return vl.get(name)
    except Exception:
        return None


@dataclass
class FabricSparkUtilsConfig:
    """Bootstrap Fabric cannot discover for itself.

    key_vault_name mirrors the role ACA's KeyVaultName environment variable plays: it is the
    one value needed to reach Key Vault, and everything else follows from there via App
    Configuration.
    """

    key_vault_name: Optional[str] = None
    variable_library_name: Optional[str] = None

    @classmethod
    def resolve(cls) -> "FabricSparkUtilsConfig":
        """Reads the Fabric variable library, falling back to environment variables.

        Missing values resolve to None rather than raising: token acquisition needs no
        configuration at all, so failing here would break callers that never touch a secret.
        The error surfaces at the point of use instead.
        """
        library = os.environ.get("FabricVariableLibrary")

        def lookup(key):
            return _get_variable_library_value(key, library) or os.environ.get(key)

        return cls(
            key_vault_name=lookup("KeyVaultName"),
            variable_library_name=library,
        )


class FabricCredentialUtils:
    """Mirrors the parts of mssparkutils.credentials this estate uses."""

    def __init__(self, config: FabricSparkUtilsConfig):
        self.config = config

    def getToken(self, audience: str, name: str = "") -> str:
        import notebookutils

        # Fabric accepts full resource scopes, so the alias table is the only
        # translation needed - anything already in scope form passes straight through.
        return notebookutils.credentials.getToken(TOKEN_AUDIENCE_SCOPES.get(audience, audience))

    def getSecret(self, akv_name: str, secret: str) -> str:
        from azure.keyvault.secrets import SecretClient

        vault_url = akv_name if akv_name.startswith("https://") else f"https://{akv_name}.vault.azure.net/"
        client = SecretClient(vault_url=vault_url, credential=FabricTokenCredential())
        value = client.get_secret(secret).value
        if value is None:
            raise ValueError(f"Secret '{secret}' not found in Key Vault '{akv_name}'.")
        return value

    def getSecretWithLS(self, linked_service: str, secret: str) -> str:
        # Fabric has no linked services; the argument is accepted and ignored so
        # existing callers need no changes. The vault comes from config instead.
        if not self.config.key_vault_name:
            raise ValueError(
                "key_vault_name is not configured. Set KeyVaultName in the Fabric "
                "variable library or as an environment variable."
            )
        return self.getSecret(self.config.key_vault_name, secret)


class FabricEnvUtils:
    """Mirrors mssparkutils.env."""

    def __init__(self, config: FabricSparkUtilsConfig):
        self.config = config

    def getWorkspaceName(self) -> str:
        """The Fabric workspace name, matching what notebookutils reports.

        Reads runtime.context rather than notebookutils.env because it is available in
        Fabric Python notebooks, which have no Spark session.
        """
        import notebookutils

        return (getattr(notebookutils.runtime, "context", None) or {}).get("currentWorkspaceName")


class FabricSparkUtils:
    def __init__(self):
        self.config = FabricSparkUtilsConfig.resolve()
        self.credentials = FabricCredentialUtils(self.config)
        self.env = FabricEnvUtils(self.config)
