# Markdown-wrapped, plain Gherkin

This file is named `.feature.md` because Fabric will not accept `.feature`
in a notebook's Resources folder. Markdown headings and prose are harmless
here: `#` is a Gherkin comment.

@markdown
Feature: Plain Gherkin in a markdown file

  Background:
    Given the semantic model "Contoso Sales"

  Scenario: A measure still evaluates
    When the measure "Margin %" is evaluated
    Then the result should be between 0 and 1
