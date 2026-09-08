"""Gherkin-based testing for Power BI semantic models.

Analysts write ``.feature`` files; everything else lives here. The notebook is a
thin wrapper::

    from corvus_python.bdd import run_tests

    result = run_tests(features="builtin/features", tags=tags, workspace=workspace)
    result.display()
    result.raise_if_failed()

This module imports without a Fabric runtime - ``sempy`` is imported lazily by
the query engine, and pandas is only needed once a run actually executes.
"""

from __future__ import annotations

from .engine import clear_cache, execute_dax, set_query_engine
from .errors import StepError
from .results import RunResult
from .runner import example_features, run_tests, validate_features

__all__ = [
    "run_tests",
    "validate_features",
    "example_features",
    "RunResult",
    "set_query_engine",
    "execute_dax",
    "clear_cache",
    "StepError",
]
