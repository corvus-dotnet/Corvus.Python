"""Shared helpers for the step modules - context access, querying, projection.

``resolve_model`` is public: project-specific step modules import it to find the
model the feature is pointed at. Everything else is internal.
"""

from __future__ import annotations

from typing import Any, List, Tuple

import pandas as pd

from .._compare import _norm_col
from ..engine import execute_dax
from ..errors import StepError

__all__ = ["resolve_model"]


def resolve_model(context: Any) -> Tuple[str, "str | None"]:
    """Return ``(dataset, workspace)`` for the model under test, or fail with a
    business-readable message if no model has been selected."""
    dataset = getattr(context, "dataset", None)
    if not dataset:
        raise StepError('No semantic model selected. Add: Given the semantic model "<name>"')
    return dataset, getattr(context, "workspace", None)


def run_query(context: Any, dax: str) -> "pd.DataFrame":
    dataset, workspace = resolve_model(context)
    df = execute_dax(dataset, dax, workspace, getattr(context, "impersonate", None))
    context.dax = dax
    context.result = df
    return df


def current_result(context: Any) -> "pd.DataFrame":
    df = getattr(context, "result", None)
    if df is None:
        raise StepError("No query has been run yet - a 'When' step is missing.")
    return df


def current_scalar(context: Any) -> Any:
    df = current_result(context)
    if df.shape[0] == 0:
        raise StepError(f"Expected a single value but the query returned no rows.\n\n{context.dax}")
    if df.shape[0] > 1 or df.shape[1] > 1:
        raise StepError(
            f"Expected a single value but got a {df.shape[0]}x{df.shape[1]} result:\n"
            f"{df.head(5).to_string(index=False)}"
        )
    value = df.iloc[0, 0]
    return None if pd.isna(value) else value


def filter_predicates(context: Any) -> List[str]:
    return list(getattr(context, "filters", []) or [])


def filters_clause(context: Any) -> str:
    filters = filter_predicates(context)
    return ("," + ",".join(f"\n    {f}" for f in filters)) if filters else ""


def model_info(context: Any, dax: str, fallback_dmv: str) -> "pd.DataFrame":
    """Run a metadata query, falling back to a ``$SYSTEM`` DMV when
    ``INFO.*`` is not available on the model."""
    dataset, workspace = resolve_model(context)
    try:
        return execute_dax(dataset, dax, workspace)
    except StepError:
        return execute_dax(dataset, fallback_dmv, workspace)


def info_names(df: "pd.DataFrame", column: str = "Name") -> "set[str]":
    lookup = {_norm_col(c): c for c in df.columns}
    key = lookup.get(_norm_col(column))
    if key is None:
        raise StepError(f"Metadata query returned no '{column}' column: {list(df.columns)}")
    return {str(v) for v in df[key].dropna()}


def table_rows(context: Any) -> List[dict]:
    """The step's Gherkin data table as a list of ``{heading: cell}`` dicts."""
    if context.table is None:
        raise StepError("This step needs a data table.")
    return [row.as_dict() for row in context.table.rows]
