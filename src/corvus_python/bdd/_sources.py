"""Reading feature files, including Markdown-wrapped ones.

Microsoft Fabric does not accept ``.feature`` files in a notebook's built-in
Resources folder, so specifications often have to be stored as ``.feature.md``
instead. behave only ever sees ``.feature`` files: the runner stages a copy into
a throwaway directory and renames it on the way through, so the ``.md`` suffix
never reaches the parser.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List

__all__ = [
    "FEATURE_SUFFIX",
    "MARKDOWN_SUFFIX",
    "FEATURE_GLOBS",
    "is_feature_file",
    "is_markdown_feature",
    "staged_name",
    "extract_gherkin",
    "read_feature",
]

FEATURE_SUFFIX = ".feature"
MARKDOWN_SUFFIX = ".feature.md"
FEATURE_GLOBS = ("*.feature", "*.feature.md")

_BOM = "\ufeff"

# A fenced code block, capturing its info string and its body. behave treats
# only `"""` and `'''` as doc-string delimiters, so a backtick fence is never
# Gherkin syntax - inside a .feature.md it can only be Markdown wrapping.
_FENCE_RE = re.compile(
    r"^[ \t]*(?:```|~~~)[ \t]*([\w+-]*)[ \t]*\r?\n(.*?)^[ \t]*(?:```|~~~)[ \t]*$",
    re.DOTALL | re.MULTILINE,
)

# Info strings that positively identify a block as the Gherkin.
_GHERKIN_INFO = {"gherkin", "feature", "cucumber"}

# Where the Gherkin starts in an unfenced Markdown document: a tag line, or the
# Feature keyword. A Markdown heading is harmless on its own (``#`` is a Gherkin
# comment) but a prose paragraph before ``Feature:`` is a parse error, so
# everything above this anchor is dropped.
_GHERKIN_START_RE = re.compile(r"^[ \t]*(?:Feature:|@[\w.\-]+(?:[ \t]+@[\w.\-]+)*[ \t]*$)")


def is_feature_file(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(MARKDOWN_SUFFIX) or name.endswith(FEATURE_SUFFIX)


def is_markdown_feature(path: Path) -> bool:
    return path.name.lower().endswith(MARKDOWN_SUFFIX)


def staged_name(name: str) -> str:
    """The file name behave should see: ``x.feature.md`` -> ``x.feature``."""
    if name.lower().endswith(MARKDOWN_SUFFIX):
        return name[: -len(".md")]
    return name


def _strip_markdown_preamble(text: str) -> str:
    """Drop any Markdown title and introduction above the first Gherkin line."""
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if _GHERKIN_START_RE.match(line):
            return "".join(lines[index:])
    return text


def extract_gherkin(text: str) -> str:
    """Pull the Gherkin out of a Markdown document.

    Two shapes are supported, because both are things people actually write once
    Fabric forces the ``.md`` suffix on them:

    * **Fenced** - the Gherkin sits in a fenced code block. Blocks tagged
      ``gherkin`` (or ``feature`` / ``cucumber``) win outright, so the document
      may also carry unrelated snippets; if no block is tagged, every block is
      used. Prose outside the fences is discarded. This is the shape to use if
      you want commentary between scenarios.
    * **Unfenced** - the file is plain Gherkin that merely happens to be named
      ``.md``, optionally under a Markdown title and introduction. Everything
      above the first tag line or ``Feature:`` is dropped, because behave
      rejects free text before ``Feature:``.
    """
    blocks = _FENCE_RE.findall(text)
    if not blocks:
        return _strip_markdown_preamble(text)
    tagged = [body for info, body in blocks if info.lower() in _GHERKIN_INFO]
    chosen: List[str] = tagged or [body for _, body in blocks]
    return "\n".join(chosen)


def read_feature(path: Path) -> str:
    """Read a feature file as text, ready to be staged for behave.

    Strips a UTF-8 BOM - behave decodes feature files as plain utf8, so a BOM
    left in place corrupts the first line, and files edited through a browser or
    copied from Windows frequently carry one.
    """
    text = path.read_text(encoding="utf-8-sig")
    if text.startswith(_BOM):  # utf-8-sig missed it (e.g. double-encoded)
        text = text[len(_BOM) :]
    if is_markdown_feature(path):
        return extract_gherkin(text)
    return text
