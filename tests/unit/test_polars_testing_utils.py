import pytest
import polars as pl
from corvus_python.testing.polars_testing_utils import (
    behave_table_to_polars_dataframe,
    behave_table_to_polars_dataframe_with_explicit_schema,
    behave_table_to_polars_dataframe_with_inferred_schema,
    behave_table_to_dictionary_by_row,
    compare_polars_dataframes,
    _string_to_polars_type,
)


class FakeRow:
    def __init__(self, cells):
        self.cells = cells


class FakeTable:
    def __init__(self, headings, rows):
        self.headings = headings
        self._rows = [FakeRow(row) for row in rows]

    def __iter__(self):
        return iter(self._rows)


def test_behave_table_to_polars_dataframe_inferred():
    table = FakeTable(["a", "b"], [["1", "2"], ["3", "4"]])
    df = behave_table_to_polars_dataframe(table)
    expected = pl.DataFrame({"a": ["1", "3"], "b": ["2", "4"]})
    compare_polars_dataframes(expected, df)


def test_behave_table_to_polars_dataframe_with_explicit_schema():
    table = FakeTable(["a:integer", "b:string"], [["1", "foo"], ["2", "bar"]])
    df = behave_table_to_polars_dataframe_with_explicit_schema(table)
    expected = pl.DataFrame({"a": [1, 2], "b": ["foo", "bar"]})
    compare_polars_dataframes(expected, df)


def test_behave_table_to_polars_dataframe_with_inferred_schema():
    table = FakeTable(["x", "y"], [["", "42"], ["hello", ""]])
    df = behave_table_to_polars_dataframe_with_inferred_schema(table)
    expected = pl.DataFrame({"x": [None, "hello"], "y": ["42", None]})
    compare_polars_dataframes(expected, df)


def test_behave_table_to_dictionary_by_row():
    table = FakeTable(["key", "val"], [["foo", "bar"], ["baz", "qux"]])
    d = behave_table_to_dictionary_by_row(table)
    assert d == {"foo": "bar", "baz": "qux"}


def test_compare_polars_dataframes_row_order():
    df1 = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pl.DataFrame({"b": ["x", "y"], "a": [1, 2]})
    compare_polars_dataframes(df1, df2, check_like=True, check_row_order=True)


def test__string_to_polars_type():
    assert _string_to_polars_type("integer") == pl.Int64
    assert _string_to_polars_type("date") == pl.Date
    assert _string_to_polars_type("boolean") == pl.Boolean
    assert _string_to_polars_type("string") == pl.Utf8
    assert _string_to_polars_type("object") == pl.Object
    assert _string_to_polars_type("decimal") == pl.Decimal
    assert _string_to_polars_type("timestamp") == pl.Datetime(time_zone="UTC")
    assert _string_to_polars_type("unknown") == pl.Utf8


def test_explicit_schema_error():
    table = FakeTable(["a", "b:string"], [["1", "foo"]])
    with pytest.raises(ValueError):
        behave_table_to_polars_dataframe_with_explicit_schema(table)
