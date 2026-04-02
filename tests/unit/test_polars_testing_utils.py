import pytest
import polars as pl
from behave.model import Table
from corvus_python.testing.polars_testing_utils import (
    behave_table_to_polars_dataframe,
    behave_table_to_polars_dataframe_with_explicit_schema,
    behave_table_to_polars_dataframe_with_inferred_schema,
    behave_table_to_dictionary_by_row,
    compare_polars_dataframes,
    _string_to_polars_type,
)


class FakeRow:
    def __init__(self, cells: list[str]):
        self.cells = cells


class FakeTable(Table):
    def __init__(self, headings: list[str], rows: list[list[str]]):
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
    expected = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    actual = pl.DataFrame({"b": ["x", "y"], "a": [1, 2]})
    compare_polars_dataframes(expected, actual, check_like=True, check_row_order=True)


def test_compare_polars_dataframes_missing_columns():
    expected = pl.DataFrame({"a": [1, 2]})
    actual = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    compare_polars_dataframes(expected, actual, check_like=True, check_row_order=True, ignore_missing_columns=True)


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


def test_explicit_schema_struct():
    table = FakeTable(
        ["data:struct<name:string,age:integer>"],
        [['{"name": "Alice", "age": 30}'], ['{"name": "Bob", "age": 25}']],
    )
    df = behave_table_to_polars_dataframe_with_explicit_schema(table)
    assert df.schema["data"] == pl.Struct({"name": pl.Utf8, "age": pl.Int64})
    assert df["data"][0] == {"name": "Alice", "age": 30}
    assert df["data"][1] == {"name": "Bob", "age": 25}


def test_explicit_schema_struct_empty_rows():
    table = FakeTable(["data:struct<name:string,age:integer>"], [])
    df = behave_table_to_polars_dataframe_with_explicit_schema(table)
    assert df.schema["data"] == pl.Struct({"name": pl.Utf8, "age": pl.Int64})
    assert len(df) == 0


def test_explicit_schema_array_of_struct():
    table = FakeTable(
        ["items:array<struct<id:integer,label:string>>"],
        [['[{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]'], ['[{"id": 3, "label": "c"}]']],
    )
    df = behave_table_to_polars_dataframe_with_explicit_schema(table)
    assert df.schema["items"] == pl.List(pl.Struct({"id": pl.Int64, "label": pl.Utf8}))
    assert df["items"].to_list() == [
        [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}],
        [{"id": 3, "label": "c"}],
    ]


def test_explicit_schema_array_of_struct_empty_rows():
    table = FakeTable(["items:array<struct<id:integer,label:string>>"], [])
    df = behave_table_to_polars_dataframe_with_explicit_schema(table)
    assert df.schema["items"] == pl.List(pl.Struct({"id": pl.Int64, "label": pl.Utf8}))
    assert len(df) == 0
