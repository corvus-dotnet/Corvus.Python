# `corvus_python.bdd` — Gherkin tests for Power BI semantic models

Executable specifications against Power BI semantic models, run from a Fabric
notebook, with the `.feature` files stored where analysts can edit them in the
browser and where Git integration will version them.

Analysts write `.feature` files. Everything else — the parser (behave), the step
library, DAX execution, reporting and result persistence — lives in this
package.

## The notebook

The wrapper notebook is a thin shell. Its whole body is:

```python
from corvus_python.bdd import run_tests

result = run_tests(
    features="builtin/features",
    tags=tags,                  # parameter cell, e.g. "@smoke and not @slow"
    workspace=workspace,
    results_table=results_table,
)
result.display()
result.raise_if_failed()
```

`builtin/features` is the notebook's built-in **Resources** folder, which since
the March 2026 release can be committed to workspace Git along with the
notebook. It contains `.feature` files and nothing else — no `steps/`, no
`environment.py`. The runner generates behave's plumbing into a throwaway
directory at run time, so it never appears in a pull request.

Queries go through `sempy.fabric.evaluate_dax`, pre-installed in the Fabric
runtime and authenticating as the notebook's executing identity — no XMLA client
libraries, no service principal, no connection strings. Row level security
scenarios additionally need `semantic-link-labs`
(`%pip install semantic-link-labs`, or add it to the attached Environment).

An example wrapper notebook and example feature files ship in
`corvus_python/bdd/examples/`. Copy them into a new notebook's Resources folder
to bootstrap a workspace.

## The step vocabulary

Test authors only use these. Everything is verified against the model at
runtime; there is no mock layer.

### Choosing what to test

| Step | Notes |
|---|---|
| `Given the semantic model "Contoso Sales"` | Usually in a `Background:` |
| `Given the semantic model "X" in the workspace "Y"` | Cross-workspace |
| `Given the workspace "Y"` | Set the workspace separately |
| `Given the report user "jo@contoso.com"` | Evaluates as that user, so RLS applies |

### Setting filter context

| Step | Notes |
|---|---|
| `Given the following filters are applied:` | Table with `Table`, `Column`, `Value`, optional `Operator` |
| `Given the filter "'Date'[Year] >= 2020"` | Raw DAX predicate escape hatch |
| `Given no filters are applied` | Clears anything from the `Background` |

### Running something

| Step | Notes |
|---|---|
| `When the measure "Total Sales" is evaluated` | Scalar, inside the current filter context |
| `When the measure "Total Sales" is evaluated by "Product[Category]"` | Grouped; comma-separate for multiple columns |
| `When the table "Product" is queried` | `EVALUATE 'Product'` |
| `When the following DAX query is executed:` | Followed by a `"""` doc string |

### Asserting on a scalar

| Step | Notes |
|---|---|
| `Then the result should be 12,477,304.29` | Compared *to the precision written*, so this passes for `12477304.2871` |
| `Then the result should be 1234.5 within 0.01` | Explicit tolerance |
| `Then the result should be "North"` | Text |
| `Then the result should be blank` / `should not be blank` | |
| `Then the result should be greater than 0` / `less than 1` / `between 0 and 1` | |

### Asserting on a table

| Step | Notes |
|---|---|
| `Then the result should be:` | Table; column names matched loosely, row order ignored |
| `Then the result should be exactly:` | As above but order-sensitive |
| `Then the result should contain:` | Subset match |
| `Then the result should have 3 rows` / `at least 3 rows` / `be empty` | |
| `Then the result should have the columns:` | Schema check |
| `Then no values should be blank` | |
| `Then the values in "Fiscal Year" should be unique` | |

Table cells are compared numerically when both sides parse as numbers, so
`8,102,110.11` matches `8102110.1132`. `<blank>` in a cell means `BLANK()`.

### Model contract tests

| Step | Notes |
|---|---|
| `Then the model should contain the measure "Total Sales"` | Via `INFO.MEASURES()`, falling back to `$SYSTEM` DMVs |
| `Then the model should not contain the measure "Old Measure"` | Catches accidental deletions and leftovers |
| `Then the model should contain the measures:` | Table of names |
| `Then the model should contain the table "Date"` | |

## The result object

`run_tests` returns a `RunResult`:

| Member | Purpose |
|---|---|
| `.passed` / `.failed` / `.total` | Scenario counts |
| `.summary()` | One-line text summary |
| `.display()` | Render the inline HTML report (Fabric `displayHTML`, falling back to `IPython`) |
| `.to_html()` | The report as a string |
| `.to_dataframe()` | One row per step — write to a Delta table for a quality trend |
| `.to_junit_xml()` | JUnit XML for an Azure DevOps / GitHub Actions test report |
| `.raise_if_failed()` | Raise `AssertionError` if anything failed, so a pipeline run goes red |

## Validating without a capacity

`validate_features()` is a dry run: it parses every feature and checks every
step resolves, without touching a semantic model. It is cheap enough to gate a
pull request in CI with no Fabric capacity — it catches the most common analyst
error, a typo that produces an undefined step.

```python
from corvus_python.bdd import validate_features

validate_features("path/to/features").raise_if_failed()
```

## Offline development and testing

`set_query_engine(fn)` swaps in any callable with the signature
`fn(dataset, dax, workspace=None, impersonate=None) -> pandas.DataFrame`, so the
whole suite runs against a fake with no capacity and no network. This is the
seam the package's own tests use (`tests/unit/bdd`).

```python
from corvus_python.bdd import run_tests, set_query_engine

set_query_engine(my_fake_engine)
result = run_tests("tests/fixtures")
```

## Extending

Project-specific steps go in a new module, never in the shipped step library.
Import the reusable helpers:

```python
# contoso_steps.py
from behave import then

from corvus_python.bdd import StepError, execute_dax
from corvus_python.bdd.steps import resolve_model


@then("the total should reconcile to the finance ledger")
def _reconcile(context):
    dataset, workspace = resolve_model(context)
    ledger = execute_dax(dataset, 'EVALUATE ROW("v", [Ledger Total])', workspace)
    ...
```

Then point `run_tests` at a features directory that also contains a `steps/`
folder importing your module, or pass it via `behave_args`. Step patterns use
behave's `parse` syntax; `"{name}"` placeholders should be written `"{name:Q}"`
to avoid `AmbiguousStep` clashes with the built-in steps, and numbers written
`{value:Number}` to keep the "compare to the precision written" behaviour.

## Deployment

Two artefacts on two cadences:

- **The package** — a versioned wheel, published to the private feed and added
  to the Fabric Spark Environment. Changes rarely, goes through a build.
- **The feature files** — the notebook's Resources folder, under workspace Git.
  Change often, edited by analysts.

Do not ship the feature files inside the wheel or move them into the
Environment's resources folder: Environment resources are outside the
Resources-in-Git support, which would defeat the requirement that analysts edit
specs under source control.

Attaching a custom Environment adds noticeable Spark session startup time, and
republishing after a library change takes minutes. That is fine for a nightly
suite. If a post-refresh smoke test needs to run in seconds, the fallback is a
Python notebook with `%pip install corvus-python` from the private feed — only
the optional "persist results to a Delta table" path needs Spark.

## Things to verify in your tenant

- The exact behaviour and limits of Resources-in-Git; it is recent and opt-in
  per notebook (Notebook settings → Git settings).
- `INFO.MEASURES()` / `INFO.TABLES()` availability on your models — the metadata
  steps fall back to `$SYSTEM.TMSCHEMA_*` DMVs, but confirm one path works.
- `sempy_labs.evaluate_dax_impersonation` argument names, if you use the RLS
  steps.
