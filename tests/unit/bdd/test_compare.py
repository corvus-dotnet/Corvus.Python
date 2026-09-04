"""Unit tests for the normalisation and frame-comparison helpers.

These carry most of the fiddly logic in the step library, so they are tested
directly rather than only through a feature file.
"""

from __future__ import annotations

import pandas as pd
import pytest

from corvus_python.bdd._compare import (
    ParsedNumber,
    _as_number,
    _compare_frames,
    _dax_literal,
    _decimals,
    _norm_col,
    _quote_table,
    parse_number,
)
from corvus_python.bdd.errors import StepError


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Sales[Total]", "total"),
        ("[Total]", "total"),
        ("'Date'[Year]", "year"),
        ("Total Sales", "total sales"),
        ("  Product[Category]  ", "category"),
    ],
)
def test_norm_col(value, expected):
    assert _norm_col(value) == expected


def test_quote_table_adds_quotes():
    assert _quote_table("Date[Year]") == "'Date'[Year]"


def test_quote_table_passes_through_quoted():
    assert _quote_table("'Date'[Year]") == "'Date'[Year]"


def test_quote_table_rejects_non_reference():
    with pytest.raises(StepError):
        _quote_table("Year")


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1,234.5", "1234.5"),
        ("42", "42"),
        ("true", "TRUE"),
        ("False", "FALSE"),
        ("<blank>", "BLANK()"),
        ("", "BLANK()"),
        ("North", '"North"'),
        ('"already quoted"', '"already quoted"'),
        ('say "hi"', '"say ""hi"""'),
    ],
)
def test_dax_literal(value, expected):
    assert _dax_literal(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1,234.56", 1234.56),
        (1234, 1234.0),
        (None, None),
        ("<blank>", None),
        ("42%", 42.0),
        ("not a number", None),
        (float("nan"), None),
    ],
)
def test_as_number(value, expected):
    assert _as_number(value) == expected


def test_decimals():
    assert _decimals("8,102,110.11") == 2
    assert _decimals("100") == 0


def test_parse_number_keeps_literal_text():
    parsed = parse_number("12,477,304.29")
    assert isinstance(parsed, ParsedNumber)
    assert parsed.text == "12,477,304.29"
    assert parsed.value == pytest.approx(12477304.29)
    assert parsed.decimals == 2
    assert float(parsed) == pytest.approx(12477304.29)


def test_compare_frames_precision_as_written():
    actual = pd.DataFrame({"Category": ["Bikes"], "Total Sales": [8102110.1132]})
    # Written to 2dp - must match despite the floating-point tail.
    _compare_frames(actual, [{"Category": "Bikes", "Total Sales": "8,102,110.11"}], subset=False, ordered=False)


def test_compare_frames_precision_mismatch_raises():
    actual = pd.DataFrame({"Total Sales": [8102110.1932]})
    with pytest.raises(StepError):
        _compare_frames(actual, [{"Total Sales": "8,102,110.11"}], subset=False, ordered=False)


def test_compare_frames_column_matched_loosely():
    actual = pd.DataFrame({"Product[Category]": ["Bikes"], "[Total Sales]": [10.0]})
    _compare_frames(actual, [{"Category": "Bikes", "Total Sales": "10"}], subset=False, ordered=False)


def test_compare_frames_unordered_match():
    actual = pd.DataFrame({"Region": ["South", "North"]})
    _compare_frames(actual, [{"Region": "North"}, {"Region": "South"}], subset=False, ordered=False)


def test_compare_frames_ordered_mismatch_raises():
    actual = pd.DataFrame({"Region": ["South", "North"]})
    with pytest.raises(StepError):
        _compare_frames(actual, [{"Region": "North"}, {"Region": "South"}], subset=False, ordered=True)


def test_compare_frames_subset_allows_extra_rows():
    actual = pd.DataFrame({"Region": ["North", "South", "West"]})
    _compare_frames(actual, [{"Region": "North"}], subset=True, ordered=False)


def test_compare_frames_non_subset_rejects_extra_rows():
    actual = pd.DataFrame({"Region": ["North", "South"]})
    with pytest.raises(StepError):
        _compare_frames(actual, [{"Region": "North"}], subset=False, ordered=False)


def test_compare_frames_missing_column_raises():
    actual = pd.DataFrame({"Region": ["North"]})
    with pytest.raises(StepError):
        _compare_frames(actual, [{"Nope": "x"}], subset=False, ordered=False)
