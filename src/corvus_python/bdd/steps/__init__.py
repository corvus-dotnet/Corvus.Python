"""The reusable Power BI step library.

Importing this package registers every step with behave's global registry and
registers the ``Number`` parse type. The runner's generated steps shim does
``from corvus_python.bdd.steps import *`` to trigger it.

``resolve_model`` is re-exported here so that project-specific step modules can
do ``from corvus_python.bdd.steps import resolve_model``.
"""

from __future__ import annotations

from behave import register_type

from .._compare import parse_number, parse_quoted
from ._helpers import resolve_model

register_type(Number=parse_number, Q=parse_quoted)

from . import model, query, assertions, tables, metadata  # noqa: E402,F401

__all__ = ["resolve_model"]
