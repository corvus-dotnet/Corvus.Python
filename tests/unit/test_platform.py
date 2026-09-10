import sys

import pytest

from corvus_python.spark_utils.platform import FABRIC, LOCAL, SYNAPSE, get_platform


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
