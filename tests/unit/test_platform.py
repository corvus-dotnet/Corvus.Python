import os
import sys

import pytest

from corvus_python.platform import FABRIC, LOCAL, SYNAPSE, configure_tls_trust_store, get_platform

_TRUSTED_FORMAT = "-----BEGIN TRUSTED CERTIFICATE-----\nMIIB\n-----END TRUSTED CERTIFICATE-----\n"
_PLAIN_FORMAT = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"


@pytest.fixture
def notebook_context(monkeypatch):
    """Sets notebookutils.runtime.context. dummy-notebookutils provides the module in tests."""
    import notebookutils

    def _set(context):
        monkeypatch.setattr(notebookutils.runtime, "context", context)

    return _set


class TestGetPlatform:
    def test_returns_local_when_notebookutils_is_absent(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "notebookutils", None)

        assert get_platform() == LOCAL

    def test_returns_fabric_when_product_type_is_fabric(self, notebook_context, monkeypatch):
        monkeypatch.delenv("MMLSPARK_PLATFORM_INFO", raising=False)
        notebook_context({"productType": "Fabric"})

        assert get_platform() == FABRIC

    def test_returns_fabric_even_though_fabric_also_sets_the_synapse_env_var(
        self, notebook_context, monkeypatch
    ):
        """Fabric Spark sessions set MMLSPARK_PLATFORM_INFO=synapse, so ordering is load-bearing.

        Reordering the checks in get_platform would misidentify Fabric as Synapse and route it
        onto the linked-service credential path, which Fabric does not support.
        """
        monkeypatch.setenv("MMLSPARK_PLATFORM_INFO", "synapse")
        notebook_context({"productType": "Fabric"})

        assert get_platform() == FABRIC

    def test_returns_synapse_when_env_var_set_and_product_type_absent(
        self, notebook_context, monkeypatch
    ):
        monkeypatch.setenv("MMLSPARK_PLATFORM_INFO", "synapse")
        notebook_context({})

        assert get_platform() == SYNAPSE

    def test_returns_local_when_context_is_empty_and_env_var_absent(
        self, notebook_context, monkeypatch
    ):
        """An empty context must not be read as Synapse.

        dummy-notebookutils ships runtime.context = {}, so a package that reached a
        non-notebook host would otherwise be routed onto the Synapse credential path.
        """
        monkeypatch.delenv("MMLSPARK_PLATFORM_INFO", raising=False)
        notebook_context({})

        assert get_platform() == LOCAL

    def test_returns_local_when_context_is_none(self, notebook_context, monkeypatch):
        monkeypatch.delenv("MMLSPARK_PLATFORM_INFO", raising=False)
        notebook_context(None)

        assert get_platform() == LOCAL

    def test_has_no_side_effect_on_the_tls_trust_store(self, notebook_context, monkeypatch):
        """get_platform is a query and must stay one.

        Configuring TLS from inside it recursed without limit on Fabric, because
        configure_tls_trust_store itself calls get_platform - taking out every caller.
        """
        monkeypatch.setenv("SSL_CERT_FILE", "/unchanged")
        notebook_context({"productType": "Fabric"})

        assert get_platform() == FABRIC
        assert os.environ["SSL_CERT_FILE"] == "/unchanged"


@pytest.fixture
def bundles(tmp_path):
    trusted = tmp_path / "ca-bundle.trust.crt"
    trusted.write_text(_TRUSTED_FORMAT)
    plain = tmp_path / "tls-ca-bundle.pem"
    plain.write_text(_PLAIN_FORMAT)
    return str(trusted), str(plain), str(tmp_path / "missing.pem")


def _given(monkeypatch, platform_name, candidates):
    monkeypatch.setattr("corvus_python.platform.get_platform", lambda: platform_name)
    monkeypatch.setattr("corvus_python.platform._TLS_BUNDLE_CANDIDATES", candidates)


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
