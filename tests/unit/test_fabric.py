import base64
import json
import time

from corvus_python.fabric import FabricTokenCredential


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
