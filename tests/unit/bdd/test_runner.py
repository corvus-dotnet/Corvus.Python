"""End-to-end tests for run_tests against the fixture feature files."""

from __future__ import annotations

import pytest

from corvus_python.bdd import RunResult, run_tests


def test_run_all_fixtures_reports_mixed_result(fixtures_dir):
    result = run_tests(fixtures_dir)
    assert isinstance(result, RunResult)
    assert result.total > 0
    assert result.passed > 0
    assert result.failed > 0  # edge-cases + undefined-step fixtures fail
    assert result.passed + result.failed == result.total


def test_sales_measures_all_pass(fixtures_dir):
    result = run_tests(fixtures_dir / "sales-measures.feature")
    assert result.failed == 0, result.summary()
    # background + 7 scenarios (one is a 3-row outline -> 9 scenario rows)
    assert result.total == 9


def test_scenario_outline_names_are_expanded(fixtures_dir):
    result = run_tests(fixtures_dir / "sales-measures.feature", tags="@sales")
    names = [sc.name for sc in result.scenarios]
    assert any("region=North" in n for n in names)


def test_failing_scenario_renders_message_expected_and_dax(fixtures_dir):
    result = run_tests(fixtures_dir / "edge-cases.feature", tags="@failing")
    assert result.failed == 2
    html = result.to_html()
    assert "Expected 999.99" in html
    # the generated DAX appears in the failure block
    assert "EVALUATE" in html and "Total Sales" in html
    # the expected table from the failing tabular scenario is rendered
    assert "Category" in html


def test_undefined_step_is_a_failure_not_a_crash(fixtures_dir):
    result = run_tests(fixtures_dir / "undefined-step.feature")
    assert result.failed == 1
    step_statuses = [st.status for sc in result.scenarios for st in sc.steps]
    assert "undefined" in step_statuses
    assert "No step definition" in result.to_html()


def test_tag_expressions_filter_scenarios(fixtures_dir):
    smoke = run_tests(fixtures_dir / "sales-measures.feature", tags="@smoke")
    # only the one @smoke scenario in sales-measures
    assert smoke.total == 1
    assert smoke.scenarios[0].name.startswith("Total sales")

    not_rls = run_tests(fixtures_dir / "sales-measures.feature", tags="@sales and not @rls")
    assert not_rls.total == 8
    assert all("@rls" not in sc.tags for sc in not_rls.scenarios)


def test_exclude_tags(fixtures_dir):
    result = run_tests(fixtures_dir / "sales-measures.feature", exclude_tags="@rls")
    assert all("@rls" not in sc.tags for sc in result.scenarios)
    assert result.total == 8


def test_to_dataframe_has_a_row_per_step(fixtures_dir):
    result = run_tests(fixtures_dir / "model-contract.feature")
    df = result.to_dataframe()
    total_steps = sum(len(sc.steps) for sc in result.scenarios)
    assert len(df) == total_steps
    assert {"feature", "scenario", "status", "step"}.issubset(df.columns)


def test_to_junit_xml_is_wellformed(fixtures_dir):
    import xml.etree.ElementTree as ET

    result = run_tests(fixtures_dir / "sales-measures.feature")
    root = ET.fromstring(result.to_junit_xml())
    assert root.tag == "testsuites"
    assert int(root.attrib["tests"]) == result.total


def test_raise_if_failed(fixtures_dir):
    passing = run_tests(fixtures_dir / "model-contract.feature")
    assert passing.raise_if_failed() is passing

    failing = run_tests(fixtures_dir / "edge-cases.feature", tags="@failing")
    with pytest.raises(AssertionError):
        failing.raise_if_failed()


def test_stop_on_first_failure(fixtures_dir):
    full = run_tests(fixtures_dir / "edge-cases.feature")
    stopped = run_tests(fixtures_dir / "edge-cases.feature", stop_on_first_failure=True)
    assert stopped.total < full.total


def test_missing_features_path_raises(fixtures_dir):
    with pytest.raises(FileNotFoundError):
        run_tests(fixtures_dir / "nope")


def test_report_shows_friendly_feature_path(fixtures_dir):
    result = run_tests(fixtures_dir / "sales-measures.feature")
    assert result.scenarios[0].feature_path.endswith("sales-measures.feature")
    assert "corvus_bdd_" not in result.scenarios[0].feature_path


def test_runs_twice_in_a_process(fixtures_dir):
    first = run_tests(fixtures_dir / "model-contract.feature")
    second = run_tests(fixtures_dir / "model-contract.feature")
    assert first.total == second.total == first.passed == second.passed
