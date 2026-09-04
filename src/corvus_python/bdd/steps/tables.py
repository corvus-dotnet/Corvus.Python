"""``Then`` steps that assert on a tabular result."""

from __future__ import annotations

from behave import then

from .._compare import _compare_frames, _norm_col
from ..errors import StepError
from ._helpers import current_result, table_rows


@then("the result should be:")
def step_table_exact(context):
    _compare_frames(current_result(context), table_rows(context), subset=False, ordered=False)


@then("the result should be exactly:")
def step_table_ordered(context):
    _compare_frames(current_result(context), table_rows(context), subset=False, ordered=True)


@then("the result should contain:")
def step_table_subset(context):
    _compare_frames(current_result(context), table_rows(context), subset=True, ordered=False)


@then("the result should have {count:d} rows")
def step_row_count(context, count):
    actual = len(current_result(context))
    if actual != count:
        raise StepError(f"Expected {count} rows but got {actual}.")


@then("the result should have at least {count:d} rows")
def step_min_rows(context, count):
    actual = len(current_result(context))
    if actual < count:
        raise StepError(f"Expected at least {count} rows but got {actual}.")


@then("the result should be empty")
def step_empty(context):
    actual = len(current_result(context))
    if actual:
        raise StepError(f"Expected no rows but got {actual}.")


@then("the result should have the columns:")
def step_columns(context):
    if context.table is None:
        raise StepError("This step needs a table of expected column names.")
    expected = [row.cells[0] for row in context.table.rows] or list(context.table.headings)
    present = {_norm_col(c) for c in current_result(context).columns}
    missing = [c for c in expected if _norm_col(c) not in present]
    if missing:
        raise StepError(
            f"Missing column(s) {missing}. " f"Present: {[str(c) for c in current_result(context).columns]}"
        )


@then("no values should be blank")
def step_no_blanks(context):
    df = current_result(context)
    bad = {str(c): int(df[c].isna().sum()) for c in df.columns if df[c].isna().any()}
    if bad:
        raise StepError(f"Blank values found: {bad}")


@then('the values in "{column:Q}" should be unique')
def step_unique(context, column):
    df = current_result(context)
    lookup = {_norm_col(c): c for c in df.columns}
    key = lookup.get(_norm_col(column))
    if key is None:
        raise StepError(f"No column '{column}' in the result.")
    duplicated = df[key][df[key].duplicated()].unique().tolist()
    if duplicated:
        raise StepError(f"Duplicate values in '{column}': {duplicated[:10]}")
