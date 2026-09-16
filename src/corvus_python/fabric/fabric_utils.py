"""Copyright (c) Endjin Limited. All rights reserved."""

import base64
import json
import os
import time
from typing import Optional

from corvus_python.platform import FABRIC, get_platform

_TLS_BUNDLE_CANDIDATES = (
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/certs/ca-certificates.crt",
)


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


def configure_tls_trust_store() -> Optional[str]:
    """Point Rust TLS clients (which Polars depends on) at a CA bundle. Returns the path set.

    Fabric sets SSL_CERT_FILE to an OpenSSL extended-trust bundle containing
    `BEGIN TRUSTED CERTIFICATE` blocks. OpenSSL reads those, so requests and the Azure
    SDKs work. rustls - used by object_store, and therefore by deltalake and polars'
    Delta reader - silently skips them, ends up with an empty root store, and fails every
    handshake with UnknownIssuer.

    No-op off Fabric, and no-op if SSL_CERT_FILE already points at a parseable PEM. Must be
    called before the first object_store request: TLS config is built once per process.
    """
    if get_platform() != FABRIC:
        return None

    current = os.environ.get("SSL_CERT_FILE")
    if current and _has_parseable_certificates(current):
        return current

    for candidate in _TLS_BUNDLE_CANDIDATES:
        if _has_parseable_certificates(candidate):
            os.environ["SSL_CERT_FILE"] = candidate
            return candidate

    return None


def _has_parseable_certificates(path: str) -> bool:
    try:
        with open(path, "r", errors="replace") as f:
            return "BEGIN CERTIFICATE-----" in f.read()
    except OSError:
        return False


def _token_expiry(token: str) -> int:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except Exception:
        return int(time.time()) + 300
