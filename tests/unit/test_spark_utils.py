import pytest

from corvus_python.platform import FABRIC
from corvus_python.spark_utils import get_spark_utils


def test_raises_on_fabric_rather_than_falling_through_to_the_local_config(monkeypatch):
    """Fabric has no mssparkutils equivalent worth returning.

    Without an explicit branch it would fall through to the local path and fail looking for
    local-spark-utils-config.json, which says nothing useful in a Fabric notebook.
    """
    monkeypatch.setattr("corvus_python.spark_utils.spark_utils.get_platform", lambda: FABRIC)

    with pytest.raises(NotImplementedError, match="corvus_python.fabric"):
        get_spark_utils()
