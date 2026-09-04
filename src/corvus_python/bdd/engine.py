"""DAX execution for the BDD step library.

The engine is a single callable with the signature::

    func(dataset, dax, workspace=None, impersonate=None) -> pandas.DataFrame

By default it runs against a live Power BI semantic model via Semantic Link
(``sempy.fabric.evaluate_dax``), or Semantic Link Labs for impersonated
(row level security) queries. ``set_query_engine`` swaps in any other callable,
which is how the whole suite runs against a fake in CI with no Fabric capacity
and no network.

``sempy`` is imported lazily so that ``import corvus_python.bdd`` works in an
environment with no Fabric runtime.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .errors import StepError

__all__ = ["set_query_engine", "clear_cache", "execute_dax"]

# (workspace, dataset, dax, impersonate) -> DataFrame
_CACHE: "dict[tuple, Any]" = {}
_ENGINE: Optional[Callable[..., Any]] = None


def _sempy_engine(dataset: str, dax: str, workspace: Optional[str] = None, impersonate: Optional[str] = None) -> Any:
    """The default engine: execute DAX against a live semantic model."""
    if impersonate:
        try:
            import sempy_labs as labs
        except ImportError as exc:  # pragma: no cover - requires Fabric runtime
            raise StepError(
                "Impersonated (row level security) queries need the "
                "semantic-link-labs library: %pip install semantic-link-labs"
            ) from exc
        return labs.evaluate_dax_impersonation(
            dataset=dataset, dax_query=dax, user_name=impersonate, workspace=workspace
        )
    import sempy.fabric as fabric  # pragma: no cover - requires Fabric runtime

    return fabric.evaluate_dax(dataset, dax, workspace=workspace)


def set_query_engine(func: Optional[Callable[..., Any]]) -> None:
    """Override how DAX is executed.

    ``func`` must accept ``(dataset, dax, workspace=None, impersonate=None)`` and
    return a ``pandas.DataFrame``. Pass ``None`` to restore the default
    Semantic Link engine. Clears the query cache.
    """
    global _ENGINE
    _ENGINE = func
    clear_cache()


def clear_cache() -> None:
    """Forget every cached query result. Called once at the start of each run."""
    _CACHE.clear()


def execute_dax(
    dataset: str, dax: str, workspace: Optional[str] = None, impersonate: Optional[str] = None, use_cache: bool = True
) -> Any:
    """Execute a DAX query and return the result as a ``pandas.DataFrame``.

    Identical queries within a run are served from a cache so that a shared
    ``Background`` costs one round trip, not one per scenario.
    """
    key = (workspace, dataset, dax.strip(), impersonate)
    if use_cache and key in _CACHE:
        return _CACHE[key]
    engine = _ENGINE or _sempy_engine
    try:
        df = engine(dataset, dax, workspace=workspace, impersonate=impersonate)
    except StepError:
        raise
    except Exception as exc:
        raise StepError(f"DAX query failed: {exc}\n\n{dax.strip()}") from exc
    if use_cache:
        _CACHE[key] = df
    return df
