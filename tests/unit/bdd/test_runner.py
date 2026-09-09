"""End-to-end tests for run_tests against the fixture feature files."""

from __future__ import annotations

from pathlib import Path

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


def test_unresolvable_builtin_path_raises_and_does_not_run_examples(tmp_path, monkeypatch):
    """A path that does not resolve must fail loudly.

    Silently falling back to the packaged example features produced a report
    that looked plausible while executing entirely different specs, which made
    edits to the real feature files look like they were being ignored.
    """
    monkeypatch.chdir(tmp_path)  # no 'builtin/features' here
    with pytest.raises(FileNotFoundError) as exc:
        run_tests("builtin/features")
    message = str(exc.value)
    assert "builtin/features" in message
    assert "/synfs/nb_resource" in message  # names every location it tried
    assert "examples" not in message


def test_example_features_are_opt_in(fixtures_dir):
    from corvus_python.bdd import example_features

    result = run_tests(example_features())
    assert result.total > 0
    assert all("examples" in p for p in result.feature_paths)


def test_feature_paths_reports_what_actually_ran(fixtures_dir):
    result = run_tests(fixtures_dir / "model-contract.feature")
    assert result.feature_paths == [f"{str(fixtures_dir).replace(chr(92), '/')}/model-contract.feature"]


def test_edits_to_a_feature_file_are_picked_up_between_runs(tmp_path):
    """The runner re-reads from disk every call - it caches nothing."""
    feature = tmp_path / "drift.feature"
    feature.write_text(
        "Feature: Drift\n"
        "  Scenario: first\n"
        '    Given the semantic model "Contoso Sales"\n'
        '    Then the model should contain the table "Sales"\n',
        encoding="utf-8",
    )
    first = run_tests(feature)
    assert [sc.name for sc in first.scenarios] == ["first"]

    feature.write_text(
        "Feature: Drift\n"
        "  Scenario: second\n"
        '    Given the semantic model "Contoso Sales"\n'
        '    Then the model should contain the table "Product"\n'
        "\n"
        "  Scenario: third\n"
        '    Given the semantic model "Contoso Sales"\n'
        '    Then the model should contain the table "Date"\n',
        encoding="utf-8",
    )
    second = run_tests(feature)
    assert [sc.name for sc in second.scenarios] == ["second", "third"]


def test_markdown_feature_files_are_discovered_and_run(fixtures_dir):
    """Fabric rejects .feature in Resources, so specs are stored as .feature.md."""
    result = run_tests(fixtures_dir / "markdown-plain.feature.md")
    assert result.failed == 0, result.summary()
    assert [sc.name for sc in result.scenarios] == ["A measure still evaluates"]


def test_markdown_feature_with_fenced_gherkin_runs(fixtures_dir):
    result = run_tests(fixtures_dir / "markdown-fenced.feature.md")
    assert result.failed == 0, result.summary()
    assert result.total == 1
    # prose and the unrelated ```sql block must not reach the parser
    assert "SELECT 1" not in result.to_html()


def test_report_shows_the_real_markdown_filename(fixtures_dir):
    result = run_tests(fixtures_dir / "markdown-fenced.feature.md")
    assert result.feature_paths == [f"{str(fixtures_dir).replace(chr(92), '/')}/markdown-fenced.feature.md"]


def test_directory_scan_picks_up_both_extensions(fixtures_dir):
    result = run_tests(fixtures_dir, tags="@markdown")
    names = {Path(p).name for p in result.feature_paths}
    assert names == {"markdown-plain.feature.md", "markdown-fenced.feature.md"}
    assert result.failed == 0, result.summary()


def test_clashing_feature_and_markdown_names_raise(tmp_path):
    body = """\
Feature: Clash
  Scenario: One
"""
    (tmp_path / "dup.feature").write_text(body, encoding="utf-8")
    (tmp_path / "dup.feature.md").write_text(body, encoding="utf-8")
    with pytest.raises(ValueError, match="both stage to"):
        run_tests(tmp_path)


def test_report_shows_friendly_feature_path(fixtures_dir):
    result = run_tests(fixtures_dir / "sales-measures.feature")
    assert result.scenarios[0].feature_path.endswith("sales-measures.feature")
    assert "corvus_bdd_" not in result.scenarios[0].feature_path


def test_runs_twice_in_a_process(fixtures_dir):
    first = run_tests(fixtures_dir / "model-contract.feature")
    second = run_tests(fixtures_dir / "model-contract.feature")
    assert first.total == second.total == first.passed == second.passed
