"""``Given`` steps: which model, whose eyes, and the filter context."""

from __future__ import annotations

from behave import given

from .._compare import _dax_literal, _quote_table
from ..errors import StepError
from ._helpers import filter_predicates, table_rows


@given('the semantic model "{dataset:Q}"')
def step_model(context, dataset):
    context.dataset = dataset
    context.filters = []


@given('the semantic model "{dataset:Q}" in the workspace "{workspace:Q}"')
def step_model_in_workspace(context, dataset, workspace):
    context.dataset = dataset
    context.workspace = workspace
    context.filters = []


@given('the workspace "{workspace:Q}"')
def step_workspace(context, workspace):
    context.workspace = workspace


@given('the report user "{user:Q}"')
def step_report_user(context, user):
    """Evaluate everything that follows as this user, so row level security applies."""
    context.impersonate = user


@given("no filters are applied")
def step_no_filters(context):
    context.filters = []


@given("the following filters are applied:")
def step_filters(context):
    filters = filter_predicates(context)
    for row in table_rows(context):
        keys = {k.lower(): v for k, v in row.items()}
        if "column" in keys and "table" in keys:
            ref = f"'{keys['table']}'[{keys['column']}]"
        elif "column" in keys:
            ref = _quote_table(keys["column"])
        else:
            raise StepError(
                "Filter tables need a 'Column' heading (optionally with 'Table'), " f"got {list(row.keys())}."
            )
        if "value" not in keys:
            raise StepError("Filter tables need a 'Value' heading.")
        operator = (keys.get("operator") or "=").strip() or "="
        filters.append(f"{ref} {operator} {_dax_literal(keys['value'])}")
    context.filters = filters


@given('the filter "{predicate:Q}"')
def step_raw_filter(context, predicate):
    context.filters = filter_predicates(context) + [predicate]
