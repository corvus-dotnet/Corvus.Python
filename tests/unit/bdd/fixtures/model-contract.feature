@contract @smoke
Feature: Sales model contract

  The report layer and downstream Excel workbooks depend on these objects existing
  with these names. Renaming or removing any of them is a breaking change.

  Background:
    Given the semantic model "Contoso Sales"

  Scenario: The published measures are all present
    Then the model should contain the measures:
      | Measure     |
      | Total Sales |
      | Margin %    |
      | Order Count |

  Scenario: The core tables are all present
    Then the model should contain the table "Sales"
    And the model should contain the table "Date"
    And the model should contain the table "Product"
