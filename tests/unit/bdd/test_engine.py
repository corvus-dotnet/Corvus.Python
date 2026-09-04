"""Tests for the pluggable query engine."""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from corvus_python.bdd import clear_cache, execute_dax, set_query_engine
from corvus_python.bdd.errors import StepError


def test_import_does_not_pull_in_sempy():
    # The package must be importable with no Fabric runtime.
    assert "sempy" not in sys.modules
    assert "sempy.fabric" not in sys.modules


def test_execute_dax_uses_override_and_caches():
    calls = []

    def engine(dataset, dax, workspace=None, impersonate=None):
        calls.append((dataset, dax))
        return pd.DataFrame({"v": [1]})

    set_query_engine(engine)
    a = execute_dax("M", "EVALUATE 1")
    b = execute_dax("M", "EVALUATE 1")
    assert a.equals(b)
    assert len(calls) == 1  # second call served from cache

    clear_cache()
    execute_dax("M", "EVALUATE 1")
    assert len(calls) == 2


def test_set_query_engine_clears_cache():
    set_query_engine(lambda *a, **k: pd.DataFrame({"v": [1]}))
    execute_dax("M", "EVALUATE 1")
    set_query_engine(lambda *a, **k: pd.DataFrame({"v": [2]}))
    assert execute_dax("M", "EVALUATE 1").iloc[0, 0] == 2


def test_engine_error_is_wrapped_as_step_error():
    def boom(*a, **k):
        raise ValueError("no such table")

    set_query_engine(boom)
    with pytest.raises(StepError) as exc:
        execute_dax("M", "EVALUATE 'Nope'")
    assert "DAX query failed" in str(exc.value)
