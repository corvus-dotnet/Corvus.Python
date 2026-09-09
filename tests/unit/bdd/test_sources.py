"""Unit tests for reading feature files, including Markdown-wrapped ones."""

from __future__ import annotations

from pathlib import Path

import pytest

from corvus_python.bdd._sources import (
    extract_gherkin,
    is_feature_file,
    is_markdown_feature,
    read_feature,
    staged_name,
)


@pytest.mark.parametrize(
    "name, expected",
    [
        ("sales.feature", True),
        ("sales.feature.md", True),
        ("SALES.FEATURE.MD", True),
        ("notes.md", False),
        ("steps.py", False),
        ("feature", False),
    ],
)
def test_is_feature_file(name, expected):
    assert is_feature_file(Path(name)) is expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ("sales.feature.md", "sales.feature"),
        ("sales.feature", "sales.feature"),
        ("a.b.feature.md", "a.b.feature"),
    ],
)
def test_staged_name_strips_only_the_md_suffix(name, expected):
    assert staged_name(name) == expected


def test_is_markdown_feature():
    assert is_markdown_feature(Path("x.feature.md"))
    assert not is_markdown_feature(Path("x.feature"))


def test_extract_gherkin_leaves_pure_gherkin_untouched():
    text = "Feature: Plain" + chr(10) + "  Scenario: One" + chr(10)
    assert extract_gherkin(text) == text


def test_extract_gherkin_drops_markdown_title_and_prose():
    """behave rejects free text before Feature:, so a preamble must go."""
    text = chr(10).join(["# A heading", "", "Some prose.", "", "@tagged", "Feature: Plain", "  Scenario: One", ""])
    result = extract_gherkin(text)
    assert result.startswith("@tagged")
    assert "Some prose." not in result
    assert "Feature: Plain" in result


def test_extract_gherkin_anchors_on_feature_when_untagged():
    text = chr(10).join(["# Title", "", "Prose.", "", "Feature: Plain", ""])
    assert extract_gherkin(text).startswith("Feature: Plain")


def test_extract_gherkin_takes_the_tagged_block_only():
    text = "Prose.\n\n" "```sql\nSELECT 1;\n```\n\n" "```gherkin\nFeature: Real\n  Scenario: One\n```\n"
    result = extract_gherkin(text)
    assert "Feature: Real" in result
    assert "SELECT 1" not in result
    assert "Prose." not in result


def test_extract_gherkin_uses_all_blocks_when_none_are_tagged():
    text = "Prose.\n\n```\nFeature: Untagged\n  Scenario: One\n```\n"
    result = extract_gherkin(text)
    assert "Feature: Untagged" in result
    assert "Prose." not in result


def test_extract_gherkin_concatenates_multiple_tagged_blocks():
    text = "```gherkin\nFeature: A\n```\n\ntext\n\n```gherkin\n  Scenario: B\n```\n"
    result = extract_gherkin(text)
    assert "Feature: A" in result and "Scenario: B" in result
    assert "text" not in result


def test_extract_gherkin_handles_tilde_fences():
    text = "~~~gherkin\nFeature: Tilde\n~~~\n"
    assert "Feature: Tilde" in extract_gherkin(text)


def test_read_feature_strips_a_utf8_bom(tmp_path):
    """behave decodes feature files as plain utf8, so a BOM corrupts line one."""
    path = tmp_path / "bom.feature"
    path.write_bytes(b"\xef\xbb\xbfFeature: With BOM\n")
    text = read_feature(path)
    assert text.startswith("Feature:")
    assert "\ufeff" not in text


def test_read_feature_extracts_from_markdown(tmp_path):
    path = tmp_path / "x.feature.md"
    path.write_text("intro\n\n```gherkin\nFeature: Extracted\n```\n", encoding="utf-8")
    assert read_feature(path).strip() == "Feature: Extracted"


def test_read_feature_leaves_plain_feature_untouched(tmp_path):
    path = tmp_path / "x.feature"
    body = "Feature: Untouched\n  Scenario: One\n"
    path.write_text(body, encoding="utf-8")
    assert read_feature(path) == body
