"""Shared fixtures for the BDD tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure the src-layout package is importable when tests run without an install.
_SRC = Path(__file__).resolve().parents[3] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from corvus_python.bdd import clear_cache, set_query_engine  # noqa: E402

from .fakes import fake_engine  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _fake_query_engine():
    """Route every DAX query through the offline fake, and reset between tests."""
    set_query_engine(fake_engine)
    clear_cache()
    yield
    set_query_engine(None)
    clear_cache()


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
