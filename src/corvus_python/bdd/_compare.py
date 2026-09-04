"""Column- and value-normalisation and data-frame comparison helpers.

These carry most of the fiddly logic in the step library and are unit tested
directly. Nothing in here touches behave or a semantic model.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List

import pandas as pd
from parse import with_pattern

from .errors import StepError

_NUMERIC_RE = re.compile(r"^[-+]?[\d,]*\.?\d+(?:[eE][-+]?\d+)?$")
_BLANKS = {"", "<blank>", "(blank)", "blank", "null", "<null>"}

# Matches a number as an analyst would write it in a feature file, e.g.
# "12,477,304.29", "-3.5", "+1000", "0.4213".
_NUMBER_PATTERN = r"[-+]?[\d,]*\.?\d+"


@dataclass(frozen=True)
class ParsedNumber:
    """A number parsed from a feature file, keeping the literal text.

    Assertions compare to *the precision written in the feature file*, so
    ``12,477,304.29`` passes for an actual value of ``12477304.2871``. That
    behaviour depends on retaining ``text`` rather than only the float.
    """

    text: str
    value: float

    def __float__(self) -> float:
        return self.value

    @property
    def decimals(self) -> int:
        return _decimals(self.text)


@with_pattern(_NUMBER_PATTERN)
def parse_number(text: str) -> ParsedNumber:
    """``parse`` type converter registered with behave as ``Number``."""
    return ParsedNumber(text=text.strip(), value=float(text.replace(",", "")))


@with_pattern(r'[^"]*')
def parse_quoted(text: str) -> str:
    """``parse`` type converter registered with behave as ``Q``.

    A quoted identifier that cannot span a ``"``. Constraining it this way stops
    behave raising ``AmbiguousStep`` between overlapping patterns such as
    ``the semantic model "{dataset}"`` and
    ``the semantic model "{dataset}" in the workspace "{workspace}"``.
    """
    return text


def _norm_col(name: Any) -> str:
    """``'Sales[Total Sales]'`` / ``'[Total Sales]'`` / ``'Total Sales'`` -> ``'total sales'``."""
    text = str(name).strip()
    if "[" in text:
        text = text[text.index("[") + 1 :]
    return text.replace("]", "").strip().strip("'").lower()


def _quote_table(ref: str) -> str:
    """``'Date[Year]'`` -> ``"'Date'[Year]"``; passes through already-quoted refs."""
    ref = ref.strip()
    if "[" not in ref:
        raise StepError(f"'{ref}' is not a column reference - use Table[Column].")
    table, column = ref.split("[", 1)
    table = table.strip().strip("'")
    return f"'{table}'[{column.strip()}"


def _dax_literal(value: str) -> str:
    """Render a feature-file cell value as a DAX literal."""
    text = str(value).strip()
    if text.lower() in ("true", "false"):
        return text.upper()
    if text.lower() in _BLANKS:
        return "BLANK()"
    if _NUMERIC_RE.match(text):
        return text.replace(",", "")
    if text.startswith('"') and text.endswith('"'):
        return text
    return '"' + text.replace('"', '""') + '"'


def _as_number(value: Any) -> "float | None":
    """Coerce a cell to ``float``, or ``None`` when it is blank / not numeric."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.lower() in _BLANKS:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def _decimals(literal: str) -> int:
    """Number of digits after the decimal point in ``literal``."""
    return len(literal.split(".")[1]) if "." in literal else 0


def _compare_frames(actual: "pd.DataFrame", expected_rows: List[dict], subset: bool, ordered: bool) -> None:
    """Compare a DAX result to a Gherkin table, matching columns by short name.

    Cells are compared numerically when both sides parse as numbers, to the
    precision written in the feature file, so ``8,102,110.11`` matches
    ``8102110.1132``. ``subset`` allows extra rows in the result; ``ordered``
    requires the rows to appear in the given order.
    """
    if not expected_rows:
        return
    wanted = list(expected_rows[0].keys())
    lookup = {_norm_col(c): c for c in actual.columns}
    missing = [c for c in wanted if _norm_col(c) not in lookup]
    if missing:
        raise StepError(
            f"The result has no column(s) {missing}. Available columns: " f"{[str(c) for c in actual.columns]}"
        )
    projected = actual[[lookup[_norm_col(c)] for c in wanted]]
    actual_rows = [{c: v for c, v in zip(wanted, row)} for row in projected.itertuples(index=False)]

    def matches(exp: dict, act: dict) -> bool:
        for col in wanted:
            e, a = exp[col], act[col]
            e_num, a_num = _as_number(e), _as_number(a)
            if e_num is not None and a_num is not None:
                places = _decimals(str(e).strip())
                if round(a_num, places) != round(e_num, places):
                    return False
            else:
                e_text = "" if str(e).strip().lower() in _BLANKS else str(e).strip()
                a_text = "" if a is None or (isinstance(a, float) and pd.isna(a)) else str(a).strip()
                if e_text != a_text:
                    return False
        return True

    if ordered and not subset:
        if len(actual_rows) != len(expected_rows):
            raise StepError(
                f"Expected {len(expected_rows)} row(s) but got {len(actual_rows)}:\n"
                f"{projected.to_string(index=False)}"
            )
        for i, (exp, act) in enumerate(zip(expected_rows, actual_rows), start=1):
            if not matches(exp, act):
                raise StepError(f"Row {i} does not match.\n  expected: {exp}\n  actual:   {act}")
        return

    remaining = list(actual_rows)
    for exp in expected_rows:
        for i, act in enumerate(remaining):
            if matches(exp, act):
                remaining.pop(i)
                break
        else:
            raise StepError(f"No row in the result matches {exp}.\n" f"{projected.head(20).to_string(index=False)}")
    if not subset and remaining:
        raise StepError(f"{len(remaining)} unexpected row(s) in the result, " f"starting with {remaining[0]}")
