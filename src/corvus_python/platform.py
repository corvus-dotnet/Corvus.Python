"""Copyright (c) Endjin Limited. All rights reserved."""

import os
from typing import Optional

FABRIC = "fabric"
SYNAPSE = "synapse"
LOCAL = "local"

_TLS_BUNDLE_CANDIDATES = (
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/certs/ca-certificates.crt",
)


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


def get_platform() -> str:
    """Return 'fabric', 'synapse' or 'local'. Requires no Spark session."""
    try:
        import notebookutils
    except ImportError:
        return LOCAL

    ctx = getattr(notebookutils.runtime, "context", None) or {}
    if ctx.get("productType") == "Fabric":
        return FABRIC

    # Only reachable once Fabric is positively excluded: Fabric Spark sessions
    # also set MMLSPARK_PLATFORM_INFO=synapse, so this is unsafe as a first check.
    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        return SYNAPSE

    # Defaults to LOCAL rather than SYNAPSE: dummy-notebookutils ships an empty
    # runtime.context, so a synapse default would misroute if it reached ACA.
    return LOCAL
