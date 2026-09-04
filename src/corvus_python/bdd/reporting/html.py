"""Inline HTML report for a :class:`~corvus_python.bdd.results.RunResult`.

Ported near-verbatim from the prototype - this is the main asset behave does
not provide. Summary banner with a proportion bar, features collapsed unless
they contain failures, every step colour-coded with its data table and doc
string beneath it, and failure blocks showing the assertion message plus the
generated DAX.
"""

from __future__ import annotations

from html import escape
from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:  # pragma: no cover
    from ..results import RunResult, ScenarioResult, TableData

_ICONS = {"passed": "✓", "failed": "✗", "skipped": "–", "undefined": "?"}


def _render_table(table: "TableData") -> str:
    head = "".join(f"<th>{escape(h)}</th>" for h in table.headings)
    body = "".join("<tr>" + "".join(f"<td>{escape(c)}</td>" for c in row) + "</tr>" for row in table.rows)
    return f"<table class=bx-tbl><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


_CSS = """
.bx{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;font-size:13px;color:#242424;
background:#fff;max-width:1080px;line-height:1.45}
.bx *{box-sizing:border-box}
.bx-hdr{border-radius:6px;padding:14px 16px;margin-bottom:12px;border:1px solid}
.bx-hdr.ok{background:#f1faf1;border-color:#9fd89f}
.bx-hdr.bad{background:#fdf3f4;border-color:#eeacb2}
.bx-badge{display:inline-block;padding:2px 10px;border-radius:11px;font-size:11px;
font-weight:700;letter-spacing:.06em;color:#fff;vertical-align:2px}
.bx-badge.ok{background:#0e700e}.bx-badge.bad{background:#b10e1c}
.bx-hdr h2{display:inline;margin:0 0 0 10px;font-size:15px;font-weight:600}
.bx-meta{margin-top:8px;color:#424242;font-size:12px}
.bx-meta b{font-weight:600}
.bx-bar{height:5px;border-radius:3px;background:#e6e6e6;margin-top:10px;overflow:hidden;display:flex}
.bx-bar i{display:block;height:100%}
.bx-bar i.ok{background:#0e700e}.bx-bar i.bad{background:#b10e1c}.bx-bar i.skip{background:#bdbdbd}
.bx-feat{border:1px solid #e0e0e0;border-radius:6px;margin-bottom:8px;overflow:hidden}
.bx-feat>summary{cursor:pointer;padding:9px 14px;font-weight:600;background:#fafafa;
list-style:none;display:flex;align-items:center;gap:10px}
.bx-feat>summary::-webkit-details-marker{display:none}
.bx-feat>summary:before{content:'\\25B8';color:#8a8886;font-size:11px;transition:none}
.bx-feat[open]>summary:before{content:'\\25BE'}
.bx-feat.bad{border-left:3px solid #b10e1c}
.bx-feat.ok{border-left:3px solid #0e700e}
.bx-path{font-weight:400;color:#8a8886;font-size:11px;font-family:Consolas,monospace;margin-left:auto}
.bx-pill{font-weight:600;font-size:11px;padding:1px 8px;border-radius:10px}
.bx-pill.ok{background:#e8f5e9;color:#0e700e}
.bx-pill.bad{background:#fde7e9;color:#b10e1c}
.bx-body{padding:4px 14px 12px}
.bx-scn{padding:9px 0;border-top:1px solid #f0f0f0}
.bx-scn:first-child{border-top:none}
.bx-scn-hd{font-weight:600;display:flex;align-items:baseline;gap:8px}
.bx-scn-hd .ic{font-size:13px}
.bx-kw{color:#8a8886;font-weight:400}
.bx-tags{font-size:11px;color:#0f6cbd;font-weight:400}
.bx-time{margin-left:auto;font-size:11px;color:#8a8886;font-weight:400}
.bx-steps{margin:6px 0 0 20px}
.bx-step{font-family:Consolas,'SF Mono',monospace;font-size:12.5px;padding:2px 0;
display:flex;gap:8px;align-items:baseline}
.bx-step .ic{width:12px;flex:none}
.bx-step b{font-weight:600;min-width:44px;display:inline-block;flex:none;text-align:right}
.bx-step.passed{color:#0e700e}
.bx-step.failed{color:#b10e1c;font-weight:600}
.bx-step.undefined{color:#9a5a00}
.bx-step.skipped{color:#a6a6a6}
.bx-arg{margin:3px 0 6px 66px}
.bx-tbl{border-collapse:collapse;font-family:Consolas,monospace;font-size:11.5px;color:#424242}
.bx-tbl th,.bx-tbl td{border:1px solid #e0e0e0;padding:2px 9px;text-align:left}
.bx-tbl th{background:#fafafa;font-weight:600}
.bx-doc{background:#fafafa;border-left:2px solid #d0d0d0;padding:6px 10px;margin:0;
font-family:Consolas,monospace;font-size:11.5px;color:#424242;white-space:pre-wrap}
.bx-err{background:#fdf3f4;border-left:3px solid #b10e1c;margin:5px 0 8px 66px;
padding:8px 12px;white-space:pre-wrap;font-family:Consolas,monospace;font-size:12px;
color:#8e0b17;border-radius:0 3px 3px 0}
.bx-err .lbl{display:block;font-family:'Segoe UI',sans-serif;font-size:11px;font-weight:700;
letter-spacing:.06em;color:#b10e1c;margin-bottom:4px}
.bx-dax{color:#605e5c;background:#fff;border:1px solid #f0d0d3;border-radius:3px;
padding:6px 9px;margin-top:8px;display:block;white-space:pre-wrap}
"""


def render_html(result: "RunResult") -> str:
    ok = result.failed == 0
    state = "ok" if ok else "bad"
    steps = [st for sc in result.scenarios for st in sc.steps]
    counts = {k: sum(1 for s in steps if s.status == k) for k in ("passed", "failed", "undefined", "skipped")}
    total_steps = len(steps) or 1

    def pct(n: int) -> float:
        return 100.0 * n / total_steps

    out: List[str] = [f"<div class=bx><style>{_CSS}</style>"]
    out.append(
        f"<div class='bx-hdr {state}'>"
        f"<span class='bx-badge {state}'>{'PASSED' if ok else 'FAILED'}</span>"
        f"<h2>{result.passed} of {result.total} scenarios passed</h2>"
        f"<div class=bx-meta>"
        f"<b>{counts['passed']}</b> steps passed &middot; "
        f"<b>{counts['failed'] + counts['undefined']}</b> failed &middot; "
        f"<b>{counts['skipped']}</b> skipped &middot; "
        f"{result.duration:.1f}s</div>"
        f"<div class=bx-bar>"
        f"<i class=ok style='width:{pct(counts['passed']):.1f}%'></i>"
        f"<i class=bad style='width:{pct(counts['failed'] + counts['undefined']):.1f}%'></i>"
        f"<i class=skip style='width:{pct(counts['skipped']):.1f}%'></i>"
        f"</div></div>"
    )

    by_feature: Dict[str, List["ScenarioResult"]] = {}
    for sc in result.scenarios:
        by_feature.setdefault(sc.feature, []).append(sc)

    for feature_name, scenarios in by_feature.items():
        bad = sum(1 for s in scenarios if s.status != "passed")
        f_state = "bad" if bad else "ok"
        pill = (
            f"<span class='bx-pill bad'>{bad} failed</span>"
            if bad
            else f"<span class='bx-pill ok'>{len(scenarios)} passed</span>"
        )
        out.append(
            f"<details class='bx-feat {f_state}'{' open' if bad else ''}>"
            f"<summary>{pill}<span><span class=bx-kw>Feature:</span> "
            f"{escape(feature_name)}</span>"
            f"<span class=bx-path>{escape(scenarios[0].feature_path)}</span>"
            f"</summary><div class=bx-body>"
        )
        for sc in scenarios:
            tags = f"<span class=bx-tags>{escape(' '.join(sc.tags))}</span>" if sc.tags else ""
            out.append(
                f"<div class=bx-scn><div class='bx-scn-hd bx-step {sc.status}'>"
                f"<span class=ic>{_ICONS.get(sc.status, '')}</span>"
                f"<span><span class=bx-kw>Scenario:</span> {escape(sc.name)}</span>{tags}"
                f"<span class=bx-time>{sc.duration:.2f}s</span></div><div class=bx-steps>"
            )
            for st in sc.steps:
                out.append(
                    f"<div class='bx-step {st.status}'>"
                    f"<span class=ic>{_ICONS.get(st.status, '')}</span>"
                    f"<b>{escape(st.step.raw_keyword)}</b>"
                    f"<span>{escape(st.step.text)}</span></div>"
                )
                if st.step.table is not None and st.status != "skipped":
                    out.append(f"<div class=bx-arg>{_render_table(st.step.table)}</div>")
                if st.step.docstring and st.status != "skipped":
                    out.append(f"<div class=bx-arg><pre class=bx-doc>" f"{escape(st.step.docstring)}</pre></div>")
                if st.error:
                    detail = f"<code class=bx-dax>{escape(st.detail.strip())}</code>" if st.detail else ""
                    label = "UNDEFINED STEP" if st.status == "undefined" else "FAILED"
                    out.append(f"<div class=bx-err><span class=lbl>{label}</span>" f"{escape(st.error)}{detail}</div>")
            out.append("</div></div>")
        out.append("</div></details>")
    out.append("</div>")
    return "".join(out)
