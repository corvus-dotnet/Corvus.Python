"""Tests for validate_features - the CI dry run."""

from __future__ import annotations

from corvus_python.bdd import set_query_engine, validate_features

from .fakes import exploding_engine


def test_validate_passes_for_good_features(fixtures_dir):
    result = validate_features(fixtures_dir / "sales-measures.feature")
    assert result.failed == 0
    assert result.total > 0


def test_validate_flags_undefined_step_without_querying(fixtures_dir):
    # If the engine is touched the test fails loudly.
    set_query_engine(exploding_engine)
    result = validate_features(fixtures_dir / "undefined-step.feature")
    assert result.failed == 1
    statuses = [st.status for sc in result.scenarios for st in sc.steps]
    assert "undefined" in statuses


def test_validate_does_not_execute_assertions(fixtures_dir):
    # edge-cases has scenarios that would fail if actually run; a dry run must
    # not execute them, so nothing fails.
    set_query_engine(exploding_engine)
    result = validate_features(fixtures_dir / "edge-cases.feature")
    assert result.failed == 0
