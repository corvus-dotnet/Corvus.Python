@edge
Feature: Edge cases

  Deliberate passes and failures, so the runner and reporter can be exercised.

  Background:
    Given the semantic model "Contoso Sales"

  @failing
  Scenario: A scalar assertion that fails
    Given the following filters are applied:
      | Table | Column      | Value  |
      | Date  | Fiscal Year | FY2025 |
    When the measure "Total Sales" is evaluated
    Then the result should be 999.99
    And the result should be greater than 0

  @failing
  Scenario: A table assertion that fails
    Given the following filters are applied:
      | Table | Column      | Value  |
      | Date  | Fiscal Year | FY2025 |
    When the measure "Total Sales" is evaluated by "Product[Category]"
    Then the result should be:
      | Category | Total Sales |
      | Bikes    | 1.00        |

  @docstring
  Scenario: Raw DAX with a doc string passes
    When the following DAX query is executed:
      """
      EVALUATE ROW("x", 1)
      """
    Then the result should be 1
