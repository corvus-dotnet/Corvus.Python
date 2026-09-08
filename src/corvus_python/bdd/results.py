"""Run results and the adapter that builds them from behave's model.

behave parses and runs; this module turns its ``feature`` / ``scenario`` /
``step`` objects into the small, stable shape that the HTML reporter and the
public API consume.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

__all__ = ["TableData", "StepInfo", "StepResult", "ScenarioResult", "RunResult"]

# behave step / scenario status name -> our vocabulary
_STATUS_MAP = {
    "passed": "passed",
    "failed": "failed",
    "error": "failed",
    "hook_error": "failed",
    "undefined": "undefined",
    "skipped": "skipped",
    "untested": "skipped",
    "executing": "skipped",
}

_OUTLINE_SUFFIX_RE = re.compile(r" -- @\d+\.\d+\s*.*$")


@dataclass
class TableData:
    """A Gherkin data table, reduced to plain strings for rendering."""

    headings: List[str]
    rows: List[List[str]]


@dataclass
class StepInfo:
    keyword: str  # normalised: Given / When / Then
    raw_keyword: str  # as authored: Given / And / But / ...
    text: str
    table: Optional[TableData] = None
    docstring: Optional[str] = None


@dataclass
class StepResult:
    step: StepInfo
    status: str  # passed | failed | skipped | undefined
    duration: float = 0.0
    error: str = ""
    detail: str = ""  # generated DAX and/or traceback


@dataclass
class ScenarioResult:
    feature: str
    feature_path: str
    name: str
    tags: List[str]
    steps: List[StepResult]
    duration: float = 0.0

    @property
    def status(self) -> str:
        if any(s.status == "failed" for s in self.steps):
            return "failed"
        if any(s.status == "undefined" for s in self.steps):
            return "undefined"
        return "passed"

    @property
    def failure_message(self) -> str:
        return "\n".join(
            f"{s.step.keyword} {s.step.text}\n  {s.error}" for s in self.steps if s.status in ("failed", "undefined")
        )


class RunResult:
    """The outcome of a run. Never raised on a test failure - call
    :meth:`raise_if_failed` to turn failures into a hard stop."""

    def __init__(self, scenarios: List[ScenarioResult], duration: float):
        self.scenarios = scenarios
        self.duration = duration

    # -- summaries -------------------------------------------------------------
    @property
    def passed(self) -> int:
        return sum(1 for s in self.scenarios if s.status == "passed")

    @property
    def failed(self) -> int:
        return sum(1 for s in self.scenarios if s.status != "passed")

    @property
    def total(self) -> int:
        return len(self.scenarios)

    @property
    def feature_paths(self) -> List[str]:
        """The feature files this run actually executed, in order.

        Worth printing when a change to a feature file does not seem to take
        effect - it says exactly which files were read.
        """
        seen: Dict[str, None] = {}
        for sc in self.scenarios:
            seen.setdefault(sc.feature_path, None)
        return list(seen)

    def summary(self) -> str:
        return f"{self.passed}/{self.total} scenarios passed " f"({self.failed} failed) in {self.duration:.1f}s"

    def __repr__(self) -> str:
        return f"<RunResult {self.summary()}>"

    # -- outputs -------------------------------------------------------------
    def to_records(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sc in self.scenarios:
            for st in sc.steps:
                rows.append(
                    {
                        "feature": sc.feature,
                        "feature_path": sc.feature_path,
                        "scenario": sc.name,
                        "tags": ",".join(sc.tags),
                        "keyword": st.step.keyword,
                        "step": st.step.text,
                        "status": st.status,
                        "duration_seconds": round(st.duration, 4),
                        "error": st.error,
                        "scenario_status": sc.status,
                    }
                )
        return rows

    def to_dataframe(self) -> Any:
        import pandas as pd

        return pd.DataFrame(self.to_records())

    def to_junit_xml(self) -> str:
        suites = ET.Element(
            "testsuites",
            {
                "tests": str(self.total),
                "failures": str(self.failed),
                "time": f"{self.duration:.3f}",
            },
        )
        by_feature: Dict[str, List[ScenarioResult]] = {}
        for sc in self.scenarios:
            by_feature.setdefault(sc.feature, []).append(sc)
        for feature_name, scenarios in by_feature.items():
            suite = ET.SubElement(
                suites,
                "testsuite",
                {
                    "name": feature_name,
                    "tests": str(len(scenarios)),
                    "failures": str(sum(1 for s in scenarios if s.status != "passed")),
                    "time": f"{sum(s.duration for s in scenarios):.3f}",
                },
            )
            for sc in scenarios:
                case = ET.SubElement(
                    suite,
                    "testcase",
                    {
                        "classname": feature_name,
                        "name": sc.name,
                        "time": f"{sc.duration:.3f}",
                    },
                )
                if sc.status != "passed":
                    ET.SubElement(case, "failure", {"message": sc.status}).text = sc.failure_message
        return ET.tostring(suites, encoding="unicode")

    def to_html(self) -> str:
        from .reporting.html import render_html

        return render_html(self)

    def display(self) -> None:
        html = self.to_html()
        try:  # Fabric / Synapse
            displayHTML(html)  # type: ignore  # noqa: F821
        except NameError:
            from IPython.display import HTML
            from IPython.display import display as _display

            _display(HTML(html))

    # -- gate --------------------------------------------------------------
    def raise_if_failed(self) -> "RunResult":
        if self.failed:
            failures = "\n\n".join(
                f"{sc.feature} :: {sc.name}\n{sc.failure_message}" for sc in self.scenarios if sc.status != "passed"
            )
            raise AssertionError(f"{self.summary()}\n\n{failures}")
        return self


# ---------------------------------------------------------------------------
# Adapter: behave model -> RunResult
# ---------------------------------------------------------------------------


def _status_name(status: Any) -> str:
    name = getattr(status, "name", str(status))
    return _STATUS_MAP.get(name, "skipped")


def _table_data(table: Any) -> Optional[TableData]:
    if table is None:
        return None
    return TableData(
        headings=[str(h) for h in table.headings],
        rows=[[str(c) for c in row.cells] for row in table.rows],
    )


def _scenario_name(scenario: Any) -> str:
    """A readable name for a scenario, expanding outlines like the prototype:
    ``Regional totals reconcile [region=North, expected=4,201,880.02]``."""
    base = _OUTLINE_SUFFIX_RE.sub("", scenario.name)
    row = getattr(scenario, "_row", None)
    if row is not None:
        pairs = ", ".join(f"{h}={v}" for h, v in zip(row.headings, row.cells))
        if pairs and pairs not in base:
            return f"{base} [{pairs}]"
    return base


def _step_result(step: Any) -> StepResult:
    status = _status_name(step.status)
    info = StepInfo(
        keyword=(getattr(step, "step_type", None) or step.keyword or "").title(),
        raw_keyword=step.keyword,
        text=step.name,
        table=_table_data(getattr(step, "table", None)),
        docstring=getattr(step, "text", None) or None,
    )
    error = step.error_message or ""
    if status == "undefined" and not error:
        error = "No step definition matches this text."

    detail = ""
    dax = getattr(step, "pbi_dax", None)
    if status == "failed" and dax and str(dax).strip() not in error:
        detail = str(dax).strip()

    return StepResult(
        step=info,
        status=status,
        duration=float(getattr(step, "duration", 0.0) or 0.0),
        error=error,
        detail=detail,
    )


def build_run_result(features: Any, path_map: Dict[str, str], duration: float, keep_all: bool = False) -> RunResult:
    """Walk behave's parsed ``features`` and produce a :class:`RunResult`.

    ``path_map`` maps the temp-directory feature path back to the path the
    caller passed, so the report shows ``builtin/features/sales-measures.feature``
    rather than a throwaway temp path. ``keep_all`` retains scenarios that did
    not run (used by ``validate_features``, where nothing executes).
    """
    scenarios: List[ScenarioResult] = []
    for feature in features:
        display_path = path_map.get(feature.filename, feature.filename)
        for scenario in feature.walk_scenarios():
            steps = [_step_result(s) for s in scenario.all_steps]
            if (
                not keep_all
                and steps
                and all(s.status == "skipped" for s in steps)
                and _status_name(scenario.status) == "skipped"
            ):
                # Filtered out by tags (or nothing ran) - omit, as the
                # prototype did, so the pass/fail counts stay meaningful.
                continue
            tags = [f"@{t}" for t in sorted(scenario.effective_tags)]
            scenarios.append(
                ScenarioResult(
                    feature=feature.name,
                    feature_path=display_path,
                    name=_scenario_name(scenario),
                    tags=tags,
                    steps=steps,
                    duration=sum(s.duration for s in steps),
                )
            )
    return RunResult(scenarios, duration)
