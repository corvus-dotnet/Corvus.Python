"""``Then`` steps that assert on a single scalar value.

Numbers are matched by a custom ``parse`` type (``Number``) that keeps the
literal text, so ``the result should be 12,477,304.29`` compares *to the
precision written* and passes for an actual value of ``12477304.2871``.
"""

from __future__ import annotations

from behave import then

from .._compare import ParsedNumber, _as_number
from ..errors import StepError
from ._helpers import current_scalar


def _expect_number(context, expected: ParsedNumber, places: int) -> float:
    actual = _as_number(current_scalar(context))
    if actual is None:
        raise StepError(f"Expected {expected.text} but the result was blank.")
    return actual


@then("the result should be {expected:Number}")
@then("the result should equal {expected:Number}")
def step_number(context, expected: ParsedNumber):
    actual = _expect_number(context, expected, expected.decimals)
    if round(actual, expected.decimals) != round(expected.value, expected.decimals):
        raise StepError(f"Expected {expected.text} but got {actual!r}.")


@then("the result should be {expected:Number} within {tolerance:Number}")
def step_number_tolerance(context, expected: ParsedNumber, tolerance: ParsedNumber):
    actual = _as_number(current_scalar(context))
    if actual is None:
        raise StepError(f"Expected {expected.text} but the result was blank.")
    if abs(actual - expected.value) > tolerance.value:
        raise StepError(f"Expected {expected.text} (+/- {tolerance.text}) but got {actual!r}.")


@then('the result should be "{expected:Q}"')
@then('the result should equal "{expected:Q}"')
def step_text(context, expected):
    actual = current_scalar(context)
    actual_text = "" if actual is None else str(actual)
    if actual_text != expected:
        raise StepError(f"Expected '{expected}' but got '{actual_text}'.")


@then("the result should be blank")
def step_blank(context):
    actual = current_scalar(context)
    if actual is not None and str(actual) != "":
        raise StepError(f"Expected a blank result but got {actual!r}.")


@then("the result should not be blank")
def step_not_blank(context):
    actual = current_scalar(context)
    if actual is None or str(actual) == "":
        raise StepError("Expected a value but the result was blank.")


@then("the result should be greater than {expected:Number}")
def step_greater_than(context, expected: ParsedNumber):
    actual = _as_number(current_scalar(context))
    if actual is None:
        raise StepError(f"Expected a number greater than {expected.text} but got a blank.")
    if actual <= expected.value:
        raise StepError(f"Expected a value greater than {expected.text} but got {actual}.")


@then("the result should be less than {expected:Number}")
def step_less_than(context, expected: ParsedNumber):
    actual = _as_number(current_scalar(context))
    if actual is None:
        raise StepError(f"Expected a number less than {expected.text} but got a blank.")
    if actual >= expected.value:
        raise StepError(f"Expected a value less than {expected.text} but got {actual}.")


@then("the result should be between {low:Number} and {high:Number}")
def step_between(context, low: ParsedNumber, high: ParsedNumber):
    actual = _as_number(current_scalar(context))
    if actual is None or not (low.value <= actual <= high.value):
        raise StepError(f"Expected a value between {low.text} and {high.text} but got {actual!r}.")
