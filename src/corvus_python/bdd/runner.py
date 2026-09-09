"""Orchestrate behave in-process and adapt its output to a :class:`RunResult`.

``run_tests`` is the whole public entry point. It copies the caller's
``.feature`` (or ``.feature.md``) files into a throwaway run directory, drops a
generated ``steps/`` shim next to them, runs behave with output capture and the
pretty formatter disabled, injects the lifecycle hooks programmatically, and
walks the parsed model to build the result.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional

from behave.configuration import Configuration
from behave.runner import Runner

from . import hooks
from ._sources import FEATURE_GLOBS, is_feature_file, read_feature, staged_name
from .results import RunResult, build_run_result

__all__ = ["run_tests", "validate_features", "example_features"]

_SHIM = Path(__file__).parent / "_templates" / "steps_shim.py.txt"

# Fabric exposes a notebook's built-in Resources folder both relative to the
# session's working directory and at an absolute mount point. Which one resolves
# depends on the notebook kind and where the session happens to be running, so a
# relative path is also tried under each of these.
_RESOURCE_MOUNTS = ("/synfs/nb_resource",)


def example_features() -> Path:
    """Path to the example feature files shipped inside the package.

    Pass this to :func:`run_tests` explicitly to run the examples. It is
    deliberately *not* a fallback for an unresolvable path - silently running
    different specs than the caller asked for hides the mistake behind a report
    that looks entirely plausible.
    """
    return (Path(__file__).parent / "examples" / "features").resolve()


def _candidate_paths(features: "str | Path") -> List[Path]:
    path = Path(features)
    candidates = [path]
    if not path.is_absolute():
        candidates += [Path(mount) / path for mount in _RESOURCE_MOUNTS]
    return candidates


def _resolve_features(features: "str | Path") -> "tuple[Path, List[Path]]":
    """Return ``(root, feature_files)``. ``features`` may be a directory of
    ``.feature`` / ``.feature.md`` files, or a single such file. ``root`` is the
    path the caller effectively passed, used to build friendly report paths.

    Raises ``FileNotFoundError`` naming every location tried if nothing
    resolves - it never substitutes different feature files.
    """
    candidates = _candidate_paths(features)
    for path in candidates:
        if path.is_dir():
            found = sorted({m for glob in FEATURE_GLOBS for m in path.rglob(glob)})
            if not found:
                raise FileNotFoundError(f"No .feature or .feature.md files under '{path}'.")
            return path.resolve(), found
        if path.is_file() and is_feature_file(path):
            return path.parent.resolve(), [path]
    tried = "\n".join(f"  - {p.as_posix()}" for p in candidates)
    raise FileNotFoundError(
        f"No feature files found for '{features}'. Tried:\n{tried}\n"
        f"Pass a directory of .feature / .feature.md files, or a single such "
        f"file. In a Fabric notebook, check the Resources folder is populated "
        f"and that the path matches what notebookutils reports."
    )


def _stage_run_dir(root: Path, feature_files: List[Path], tmp: Path) -> Dict[str, str]:
    """Copy the feature files into ``<tmp>/features`` preserving their path
    relative to ``root``, write the steps shim, and return
    {staged temp path -> friendly display path}."""
    run_features = tmp / "features"
    (run_features / "steps").mkdir(parents=True)
    (run_features / "steps" / "all_steps.py").write_text(_SHIM.read_text(encoding="utf-8"), encoding="utf-8")

    display_root = str(root).replace("\\", "/")
    path_map: Dict[str, str] = {}
    staged_from: Dict[str, Path] = {}
    for src in feature_files:
        src = src.resolve()
        try:
            rel = src.relative_to(root)
        except ValueError:
            rel = Path(src.name)

        # behave only discovers *.feature, so a Markdown-wrapped spec is staged
        # under its un-suffixed name. The report still shows the real file.
        staged_rel = rel.with_name(staged_name(rel.name))
        dest = run_features / staged_rel

        clash = staged_from.get(str(dest))
        if clash is not None:
            raise ValueError(
                f"'{rel.as_posix()}' and '{clash.as_posix()}' both stage to "
                f"'{staged_rel.as_posix()}'. Rename one - a .feature and a "
                f".feature.md of the same name cannot live side by side."
            )
        staged_from[str(dest)] = rel

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(read_feature(src), encoding="utf-8")
        path_map[str(dest)] = f"{display_root}/{rel.as_posix()}"
    return path_map


def _build_args(
    features_dir: Path,
    *,
    tags: Optional[str],
    exclude_tags: Optional[str],
    stop: bool,
    dry_run: bool,
    behave_args: Optional[List[str]],
) -> List[str]:
    args = [
        str(features_dir),
        "--no-capture",
        "--no-capture-stderr",
        "--no-logcapture",
        "--format",
        "null",
    ]
    if tags:
        args += ["--tags", tags]
    if exclude_tags:
        args += ["--tags", f"not ({exclude_tags})"]
    if stop:
        args += ["--stop"]
    if dry_run:
        args += ["--dry-run"]
    if behave_args:
        args += list(behave_args)
    return args


def _run(
    features: "str | Path",
    *,
    tags: Optional[str],
    exclude_tags: Optional[str],
    workspace: Optional[str],
    results_table: Optional[str],
    stop: bool,
    dry_run: bool,
    behave_args: Optional[List[str]],
) -> RunResult:
    root, feature_files = _resolve_features(features)
    with tempfile.TemporaryDirectory(prefix="corvus_bdd_") as tmpdir:
        tmp = Path(tmpdir)
        path_map = _stage_run_dir(root, feature_files, tmp)
        run_features = tmp / "features"

        args = _build_args(
            run_features,
            tags=tags,
            exclude_tags=exclude_tags,
            stop=stop,
            dry_run=dry_run,
            behave_args=behave_args,
        )
        config = Configuration(command_args=args, load_config=False)
        config.userdata.update(
            {k: v for k, v in (("workspace", workspace), ("results_table", results_table)) if v is not None}
        )
        # Silence behave's own stdout summary; we render our own report.
        config.reporters = []

        runner = Runner(config)
        runner.hooks["before_all"] = hooks.before_all
        runner.hooks["before_scenario"] = hooks.before_scenario
        runner.hooks["after_step"] = hooks.after_step

        cwd = os.getcwd()
        start = time.time()
        try:
            runner.run()
        finally:
            os.chdir(cwd)
        duration = time.time() - start

        # Resolve temp paths that behave may have normalised.
        resolved_map = dict(path_map)
        for feature in runner.features:
            if feature.filename not in resolved_map:
                match = next(
                    (
                        d
                        for p, d in path_map.items()
                        if os.path.normcase(os.path.abspath(p)) == os.path.normcase(os.path.abspath(feature.filename))
                    ),
                    None,
                )
                if match:
                    resolved_map[feature.filename] = match

        result = build_run_result(runner.features, resolved_map, duration, keep_all=dry_run)

    if results_table and not dry_run:
        from .persistence import save_results

        save_results(result, results_table, workspace=workspace)

    return result


def run_tests(
    features: "str | Path" = "builtin/features",
    *,
    tags: Optional[str] = None,
    exclude_tags: Optional[str] = None,
    workspace: Optional[str] = None,
    results_table: Optional[str] = None,
    stop_on_first_failure: bool = False,
    behave_args: Optional[List[str]] = None,
) -> RunResult:
    """Run the Gherkin specifications under ``features`` against live semantic
    models and return a :class:`RunResult`.

    ``tags`` / ``exclude_tags`` are behave tag expressions, e.g.
    ``"@smoke and not @slow"``. ``results_table``, if given, appends the run to
    that Delta table. Never raises on a test failure - call
    ``result.raise_if_failed()`` to gate.
    """
    return _run(
        features,
        tags=tags,
        exclude_tags=exclude_tags,
        workspace=workspace,
        results_table=results_table,
        stop=stop_on_first_failure,
        dry_run=False,
        behave_args=behave_args,
    )


def validate_features(
    features: "str | Path" = "builtin/features",
    *,
    tags: Optional[str] = None,
    exclude_tags: Optional[str] = None,
    behave_args: Optional[List[str]] = None,
) -> RunResult:
    """Dry run: parse every feature and check every step resolves, without
    touching a semantic model. Cheap enough to gate a pull request in CI with
    no Fabric capacity. Undefined steps surface as failures in the result."""
    return _run(
        features,
        tags=tags,
        exclude_tags=exclude_tags,
        workspace=None,
        results_table=None,
        stop=False,
        dry_run=True,
        behave_args=behave_args,
    )
