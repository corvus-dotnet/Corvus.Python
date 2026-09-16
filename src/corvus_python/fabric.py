"""Copyright (c) Endjin Limited. All rights reserved."""

import base64
import json
import time
from typing import Optional


class FabricTokenCredential:
    """azure-identity compatible TokenCredential backed by notebookutils.

    A Fabric notebook has no IMDS endpoint and no Azure CLI, so DefaultAzureCredential has no
    source to authenticate from. This lets the Azure SDKs be used there instead, with the request
    issued by the notebook process itself - so it traverses any managed private endpoint the
    workspace has.
    """

    def get_token(self, *scopes, **kwargs):
        from azure.core.credentials import AccessToken
        import notebookutils

        # Fabric rejects short keyword audiences such as "synapse" but accepts full resource
        # scopes, so the scope the SDK asks for is passed through unchanged. kwargs (claims,
        # tenant_id, enable_cae) are intentionally ignored: notebookutils issues tokens for the
        # executing identity only and cannot satisfy a CAE challenge.
        token = notebookutils.credentials.getToken(scopes[0])
        return AccessToken(token, _token_expiry(token))


def get_variable_library_value(name: str, library_name: str) -> Optional[str]:
    """Reads a variable from a named Fabric variable library.

    Returns None when the value or the library cannot be read, including off Fabric, so callers
    can fall back to environment variables.
    """
    try:
        import notebookutils

        return notebookutils.variableLibrary.get(f"$(/**/{library_name}/{name})")
    except Exception:
        return None


def _token_expiry(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return int(time.time()) + 300
