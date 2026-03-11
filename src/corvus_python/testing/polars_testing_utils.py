"""
Utility functions for testing: Behave table to Polars DataFrame conversions and comparisons.
"""

from typing import Any
import polars as pl
import polars.testing as pl_testing
from behave.model import Table
from opentelemetry import trace

from ..tracing import start_as_current_span_with_method_name

tracer = trace.get_tracer(__name__)


@start_as_current_span_with_method_name(tracer)
def behave_table_to_polars_dataframe(table: Any) -> pl.DataFrame:
    """
    Converts a Behave table to a Polars DataFrame.
    This function infers the schema if column types are not explicitly provided
    in the headings (e.g., "column_name:type").
    Args:
        table: The Behave table object.
    Returns:
        A Polars DataFrame.
    """
    if ":" in table.headings[0]:
        return behave_table_to_polars_dataframe_with_explicit_schema(table)
    else:
        return behave_table_to_polars_dataframe_with_inferred_schema(table)


@start_as_current_span_with_method_name(tracer)
def behave_table_to_polars_dataframe_with_explicit_schema(table: Any) -> pl.DataFrame:
    cols = [h.split(":", 1) for h in table.headings]  # Split only on first colon
    if any(len(c) != 2 for c in cols):
        raise ValueError("field_name:field_type expected in table headings")

    schema = {name: _string_to_polars_type(field_type) for name, field_type in cols}
    rows = [{name: cell for (name, _), cell in zip(cols, row.cells)} for row in table]

    if not rows:
        return pl.DataFrame(schema=schema)
    else:
        df = pl.DataFrame(rows)

    for name, field_type in cols:
        df = df.with_columns(pl.when(pl.col(name) == "nan").then(None).otherwise(pl.col(name)).alias(name))
        try:
            if field_type.lower().startswith("struct<"):
                df = df.with_columns(pl.col(name).str.json_decode().alias(name))
            elif field_type.lower().startswith("array<struct<"):
                df = df.with_columns(pl.col(name).str.json_decode().alias(name))
            elif field_type.lower() == "date":
                df = df.with_columns(pl.col(name).str.to_date())
            elif field_type.lower().startswith("date("):
                format_str = (
                    field_type.split("(")[1].strip(")").replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
                )
                df = df.with_columns(pl.col(name).str.to_date(format=format_str))
            elif field_type.lower() in ["datetime", "timestamp"]:
                df = df.with_columns(
                    pl.col(name)
                    .str.replace(" ", "T", literal=True)
                    .str.strptime(
                        pl.Datetime(time_zone="UTC"),
                        format="%Y-%m-%dT%H:%M:%S%.f",
                        strict=False,
                    )
                )
            elif field_type.lower().startswith("decimal("):
                scale_part = field_type.split(",")[1]
                scale = int(scale_part.strip(")"))
                precision_part = field_type.split("(")[1]
                precision = int(precision_part.split(",")[0])
                df = df.with_columns(pl.col(name).cast(pl.Decimal(precision=precision, scale=scale)))
            elif schema[name] == pl.Boolean:
                df = df.with_columns(
                    pl.when(pl.col(name) == "True")
                    .then(True)
                    .when(pl.col(name) == "False")
                    .then(False)
                    .otherwise(None)
                    .alias(name)
                    .cast(pl.Boolean)
                )
            elif schema[name] == pl.Decimal:
                df = df.with_columns(pl.col(name).cast(pl.Decimal(scale=2)))
            elif schema[name] != pl.Object:
                df = df.with_columns(pl.col(name).cast(schema[name]))
        except Exception as e:
            raise ValueError(f"Error converting column {name} to type {field_type}: {e}")
    return df


@start_as_current_span_with_method_name(tracer)
def behave_table_to_polars_dataframe_with_inferred_schema(table: Any) -> pl.DataFrame:
    """
    Converts a Behave table to a Polars DataFrame with inferred schema.
    Args:
        table: The Behave table object.
    Returns:
        A Polars DataFrame with inferred schema.
    """
    headings = table.headings
    rows = [{headings[i]: cell for i, cell in enumerate(row.cells)} for row in table]
    for row in rows:
        for key, value in row.items():
            if value == "":
                row[key] = None
    return pl.DataFrame(rows)


@start_as_current_span_with_method_name(tracer)
def behave_table_to_dictionary_by_row(table: Table) -> dict[str, str]:
    """
    Converts a Behave table with two columns into a dictionary.
    The first column is used as keys and the second as values.
    Args:
        table: The Behave table object (expected to have two columns).
    Returns:
        A dictionary where keys are from the first column and values are from the second.
    """
    return {row.cells[0]: row.cells[1] for row in table}


@start_as_current_span_with_method_name(tracer)
def compare_polars_dataframes(
    expected: pl.DataFrame,
    actual: pl.DataFrame,
    check_like: bool = True,
    check_row_order: bool = True,
):
    """
    Compares two Polars DataFrames for equality.
    Args:
        expected: The expected Polars DataFrame.
        actual: The actual Polars DataFrame.
        check_like: If True, columns will be reordered to match `expected`.
        check_row_order: If True, row order will be checked.
    """
    expected = expected.select(sorted(expected.columns)).sort(by=expected.columns)
    actual = actual.select(sorted(actual.columns)).sort(by=actual.columns)
    if check_like:
        actual = actual.select(expected.columns)
    pl_testing.assert_frame_equal(expected, actual, check_row_order=check_row_order)


def _string_to_polars_type(type_name: str) -> pl.DataType:
    """
    Converts a string representation of a type to a Polars DataType.
    Args:
        type_name: The string name of the type (e.g., "integer", "string", "date").
    Returns:
        The corresponding Polars DataType.
    """
    type_name_lower = type_name.lower()
    if type_name_lower.startswith("date"):
        return pl.Date
    type_map = {
        "integer": pl.Int64,
        "long": pl.Int64,
        "integer8": pl.Int8,
        "integer32": pl.Int32,
        "float": pl.Float64,
        "double": pl.Float64,
        "boolean": pl.Boolean,
        "timestamp": pl.Datetime(time_zone="UTC"),
        "string": pl.Utf8,
        "object": pl.Object,
        "decimal": pl.Decimal,
    }
    return type_map.get(type_name_lower, pl.Utf8)
