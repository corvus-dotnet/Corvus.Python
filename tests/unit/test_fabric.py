import base64
import json
import os
import time

import pytest

from corvus_python.fabric import FabricTokenCredential, configure_tls_trust_store
from corvus_python.platform import FABRIC, LOCAL

_TRUSTED_FORMAT = "-----BEGIN TRUSTED CERTIFICATE-----\nMIIB\n-----END TRUSTED CERTIFICATE-----\n"
_PLAIN_FORMAT = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"


def _jwt(expires_on: int) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": expires_on}).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def _fake_credentials(monkeypatch, token, requested=None):
    class FakeCredentials:
        @staticmethod
        def getToken(audience):
            if requested is not None:
                requested.append(audience)
            return token

    monkeypatch.setattr("notebookutils.credentials", FakeCredentials)


class TestFabricTokenCredential:
    def test_passes_the_requested_scope_through_unchanged(self, monkeypatch):
        """Fabric rejects short keyword audiences but accepts full resource scopes."""
        requested = []
        _fake_credentials(monkeypatch, _jwt(int(time.time()) + 3600), requested)

        FabricTokenCredential().get_token("https://dev.azuresynapse.net/.default")

        assert requested == ["https://dev.azuresynapse.net/.default"]

    def test_reads_the_expiry_from_the_token(self, monkeypatch):
        expires_on = int(time.time()) + 1234
        _fake_credentials(monkeypatch, _jwt(expires_on))

        assert FabricTokenCredential().get_token("scope").expires_on == expires_on

    def test_falls_back_to_a_short_expiry_when_the_token_cannot_be_parsed(self, monkeypatch):
        _fake_credentials(monkeypatch, "not-a-jwt")

        token = FabricTokenCredential().get_token("scope")

        assert token.token == "not-a-jwt"
        assert token.expires_on > int(time.time())


@pytest.fixture
def bundles(tmp_path):
    trusted = tmp_path / "ca-bundle.trust.crt"
    trusted.write_text(_TRUSTED_FORMAT)
    plain = tmp_path / "tls-ca-bundle.pem"
    plain.write_text(_PLAIN_FORMAT)
    return str(trusted), str(plain), str(tmp_path / "missing.pem")


def _given(monkeypatch, platform_name, candidates):
    # Patched where configure_tls_trust_store looks them up, not on the corvus_python.platform
    # package, which would leave the name fabric_utils imported for itself untouched.
    monkeypatch.setattr("corvus_python.fabric.fabric_utils.get_platform", lambda: platform_name)
    monkeypatch.setattr("corvus_python.fabric.fabric_utils._TLS_BUNDLE_CANDIDATES", candidates)


class TestConfigureTlsTrustStore:
    def test_is_a_no_op_off_fabric(self, monkeypatch, bundles):
        trusted, plain, _ = bundles
        _given(monkeypatch, LOCAL, (plain,))
        monkeypatch.setenv("SSL_CERT_FILE", trusted)

        assert configure_tls_trust_store() is None
        assert os.environ["SSL_CERT_FILE"] == trusted

    def test_replaces_an_openssl_trust_format_bundle(self, monkeypatch, bundles):
        """Fabric's default bundle uses BEGIN TRUSTED CERTIFICATE blocks, which rustls skips."""
        trusted, plain, _ = bundles
        _given(monkeypatch, FABRIC, (plain,))
        monkeypatch.setenv("SSL_CERT_FILE", trusted)

        assert configure_tls_trust_store() == plain
        assert os.environ["SSL_CERT_FILE"] == plain

    def test_leaves_an_already_parseable_bundle_alone(self, monkeypatch, bundles, tmp_path):
        _, plain, _ = bundles
        other = tmp_path / "other.pem"
        other.write_text(_PLAIN_FORMAT)
        _given(monkeypatch, FABRIC, (str(other),))
        monkeypatch.setenv("SSL_CERT_FILE", plain)

        assert configure_tls_trust_store() == plain
        assert os.environ["SSL_CERT_FILE"] == plain

    def test_skips_missing_and_unparseable_candidates(self, monkeypatch, bundles):
        trusted, plain, missing = bundles
        _given(monkeypatch, FABRIC, (missing, trusted, plain))
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)

        assert configure_tls_trust_store() == plain

    def test_returns_none_when_no_candidate_is_parseable(self, monkeypatch, bundles):
        trusted, _, missing = bundles
        _given(monkeypatch, FABRIC, (missing, trusted))
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)

        assert configure_tls_trust_store() is None
        assert "SSL_CERT_FILE" not in os.environ
