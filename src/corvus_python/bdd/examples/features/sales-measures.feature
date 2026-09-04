@sales @nightly
Feature: Sales measures

  As a finance analyst
  I want the headline sales measures to be correct
  So that I can trust the numbers I report to the board

  Background:
    Given the semantic model "Contoso Sales"

  @smoke
  Scenario: Total sales for the last closed financial year
    Given the following filters are applied:
      | Table  | Column       | Value   |
      | Date   | Fiscal Year  | FY2025  |
    When the measure "Total Sales" is evaluated
    Then the result should be 12,477,304.29

  Scenario: Margin percentage is reported as a ratio, not a percentage
    Given the following filters are applied:
      | Table  | Column       | Value   |
      | Date   | Fiscal Year  | FY2025  |
    When the measure "Margin %" is evaluated
    Then the result should be between 0 and 1

  Scenario: Sales split by product category
    Given the following filters are applied:
      | Table  | Column       | Value   |
      | Date   | Fiscal Year  | FY2025  |
    When the measure "Total Sales" is evaluated by "Product[Category]"
    Then the result should be:
      | Category    | Total Sales  |
      | Bikes       | 8,102,110.11 |
      | Accessories | 2,880,194.18 |
      | Clothing    | 1,495,000.00 |

  Scenario Outline: Regional totals reconcile to the source system
    Given the following filters are applied:
      | Table  | Column       | Value      |
      | Date   | Fiscal Year  | FY2025     |
      | Store  | Region       | <region>   |
    When the measure "Total Sales" is evaluated
    Then the result should be <expected>

    Examples:
      | region | expected     |
      | North  | 4,201,880.02 |
      | South  | 3,995,110.55 |
      | West   | 4,280,313.72 |

  Scenario: Year on year growth handles the first year without erroring
    Given the following filters are applied:
      | Table | Column      | Value  |
      | Date  | Fiscal Year | FY2019 |
    When the measure "Sales YoY %" is evaluated
    Then the result should be blank

  @rls
  Scenario: A regional manager only sees their own region
    Given the report user "north.manager@contoso.com"
    When the measure "Total Sales" is evaluated by "Store[Region]"
    Then the result should have 1 rows
    And the result should contain:
      | Region |
      | North  |

  Scenario: Bespoke logic can still be expressed in raw DAX
    When the following DAX query is executed:
      """
      EVALUATE
      SUMMARIZECOLUMNS(
          'Date'[Fiscal Year],
          "Orders", [Order Count],
          "Average Order Value", [Total Sales] / [Order Count]
      )
      ORDER BY 'Date'[Fiscal Year]
      """
    Then the result should have at least 5 rows
    And no values should be blank
    And the values in "Fiscal Year" should be unique
