"""``Then`` steps that assert on the model's contract - which measures and
tables it publishes. Uses ``INFO.MEASURES()`` / ``INFO.TABLES()``, falling back
to the ``$SYSTEM.TMSCHEMA_*`` DMVs."""

from __future__ import annotations

from behave import then

from ..errors import StepError
from ._helpers import info_names, model_info, table_rows

_MEASURES = ("EVALUATE INFO.MEASURES()", "SELECT [Name] FROM $SYSTEM.TMSCHEMA_MEASURES")
_TABLES = ("EVALUATE INFO.TABLES()", "SELECT [Name] FROM $SYSTEM.TMSCHEMA_TABLES")


@then('the model should contain the measure "{measure:Q}"')
def step_has_measure(context, measure):
    df = model_info(context, *_MEASURES)
    if measure not in info_names(df):
        raise StepError(f"The model has no measure called '{measure}'.")


@then('the model should not contain the measure "{measure:Q}"')
def step_no_measure(context, measure):
    df = model_info(context, *_MEASURES)
    if measure in info_names(df):
        raise StepError(f"The measure '{measure}' still exists in the model.")


@then('the model should contain the table "{table:Q}"')
def step_has_table(context, table):
    df = model_info(context, *_TABLES)
    if table not in info_names(df):
        raise StepError(f"The model has no table called '{table}'.")


@then("the model should contain the measures:")
def step_has_measures(context):
    df = model_info(context, *_MEASURES)
    present = info_names(df)
    rows = table_rows(context)
    expected = [next(iter(row.values())) for row in rows]
    missing = [m for m in expected if m not in present]
    if missing:
        raise StepError(f"Missing measure(s): {missing}")
