# Sales contract, fenced

Prose that must not reach the Gherkin parser, including a fenced snippet in
another language:

```sql
SELECT 1;
```

The specification itself:

```gherkin
@markdown @fenced
Feature: Fenced Gherkin in a markdown file

  Background:
    Given the semantic model "Contoso Sales"

  Scenario: The core tables are present
    Then the model should contain the table "Sales"
    And the model should contain the table "Date"
```
