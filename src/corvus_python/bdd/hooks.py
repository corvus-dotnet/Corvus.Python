"""behave lifecycle hooks, injected programmatically by the runner.

They are assigned onto ``runner.hooks`` rather than shipped as an
``environment.py`` in the Resources folder, so that analysts only ever see
``.feature`` files.
"""

from __future__ import annotations

from typing import Any

from .engine import clear_cache


def before_all(context: Any) -> None:
    """Stash run-level config onto the context and start with a clean cache."""
    userdata = getattr(context.config, "userdata", {}) or {}
    context.workspace = userdata.get("workspace") or None
    context.results_table = userdata.get("results_table") or None
    clear_cache()


def before_scenario(context: Any, scenario: Any) -> None:
    """Reset per-scenario query state.

    behave already pushes and pops a scenario-level layer, so filter context set
    by a ``Given`` does not leak between scenarios; this just guarantees the
    helpers always see a defined ``filters`` list and no stale result.
    """
    context.filters = []
    context.dax = None
    context.result = None


def after_step(context: Any, step: Any) -> None:
    """Record the DAX that ran on the step object so the reporter can show it.

    behave's model objects accept arbitrary attributes, and this is the only
    point at which both the step and the context are in scope.
    """
    step.pbi_dax = getattr(context, "dax", None)
