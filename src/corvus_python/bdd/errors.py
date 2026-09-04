"""Exceptions raised by the BDD step library."""

from __future__ import annotations


class StepError(AssertionError):
    """Raised by step implementations to signal a business-readable failure.

    Subclasses :class:`AssertionError` so that behave treats it as a failing
    assertion rather than an unexpected error, and so that the message is shown
    verbatim in the report.
    """
