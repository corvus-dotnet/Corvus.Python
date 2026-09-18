import notebookutils

from corvus_python.platform import FABRIC, SYNAPSE
from corvus_python.spark_utils import get_spark_utils


def test_returns_fabrics_native_notebookutils_on_fabric(monkeypatch):
    """Fabric's native API is the flattened notebookutils, which works in both Spark and Python notebooks.

    mssparkutils is only a compatibility alias on Fabric. And without an explicit Fabric branch, a Fabric
    session would fall through to the local path and fail looking for local-spark-utils-config.json.
    """
    monkeypatch.setattr("corvus_python.spark_utils.spark_utils.get_platform", lambda: FABRIC)

    assert get_spark_utils() is notebookutils


def test_returns_mssparkutils_on_synapse(monkeypatch):
    from notebookutils import mssparkutils

    monkeypatch.setattr("corvus_python.spark_utils.spark_utils.get_platform", lambda: SYNAPSE)

    assert get_spark_utils() is mssparkutils
