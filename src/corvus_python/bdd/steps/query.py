"""``When`` steps: evaluate a measure, query a table, or run raw DAX."""

from __future__ import annotations

from behave import when

from .._compare import _quote_table
from ..errors import StepError
from ._helpers import filter_predicates, filters_clause, run_query


@when('the measure "{measure:Q}" is evaluated')
def step_measure(context, measure):
    filters = filter_predicates(context)
    name = measure.strip("[]")
    inner = f"[{name}]"
    if filters:
        inner = "CALCULATE(" + inner + "," + ", ".join(filters) + ")"
    run_query(context, f'EVALUATE\nROW("{name}", {inner})')


@when('the measure "{measure:Q}" is evaluated by "{group_by:Q}"')
def step_measure_by(context, measure, group_by):
    columns = ", ".join(_quote_table(c) for c in group_by.split(","))
    name = measure.strip("[]")
    dax = f"EVALUATE\nSUMMARIZECOLUMNS(\n    {columns},\n" f'    "{name}", [{name}]{filters_clause(context)}\n)'
    run_query(context, dax)


@when('the table "{table:Q}" is queried')
def step_table(context, table):
    filters = filter_predicates(context)
    expr = f"'{table.strip(chr(39))}'"
    if filters:
        expr = f"CALCULATETABLE({expr}, {', '.join(filters)})"
    run_query(context, f"EVALUATE\n{expr}")


@when("the following DAX query is executed:")
def step_raw_dax(context):
    if not context.text:
        raise StepError('This step needs a DAX query in a """ block.')
    run_query(context, context.text)
