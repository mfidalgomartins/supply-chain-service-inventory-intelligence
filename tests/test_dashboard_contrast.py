"""WCAG AA contrast guard for the dashboard colour system.

Parses the design tokens straight out of the generated HTML for each theme and
asserts that the foreground/background pairs that actually carry text meet the
WCAG 2.1 AA ratio (4.5:1 for normal text). This re-reads whatever values the
template currently defines, so it stays correct as the palette evolves and only
fails when a chosen colour is no longer accessible.
"""

from __future__ import annotations

import re

import pytest
from src.executive_dashboard import _build_html

AA_NORMAL = 4.5


def _relative_luminance(hex_color: str) -> float:
    channels = []
    for component in (hex_color[1:3], hex_color[3:5], hex_color[5:7]):
        srgb = int(component, 16) / 255
        channels.append(srgb / 12.92 if srgb <= 0.03928 else ((srgb + 0.055) / 1.055) ** 2.4)
    r, g, b = channels
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _contrast(fg: str, bg: str) -> float:
    lo, hi = sorted((_relative_luminance(fg), _relative_luminance(bg)))
    return (hi + 0.05) / (lo + 0.05)


def _tokens_for(theme_block: str) -> dict[str, str]:
    return dict(re.findall(r"--([\w-]+):\s*(#[0-9a-fA-F]{6})\b", theme_block))


def _theme_blocks() -> dict[str, dict[str, str]]:
    # _build_html only serialises the payload and substitutes it into the
    # template; it never reads payload keys, so an empty payload yields the same
    # static design-token blocks we want to audit.
    html = _build_html({})
    light = re.search(r":root\s*\{(.*?)\}", html, re.S)
    dark = re.search(r'\[data-theme="dark"\]\s*\{(.*?)\}', html, re.S)
    assert light and dark, "could not locate token blocks in generated HTML"
    return {"light": _tokens_for(light.group(1)), "dark": _tokens_for(dark.group(1))}


# (foreground token, background token) pairs that render text on a surface.
TEXT_PAIRS = [
    ("ink", "surface"),
    ("ink-soft", "surface"),
    ("muted", "surface"),
    ("muted", "inset"),
    ("faint", "surface"),
    ("faint", "bg"),
    ("accent", "surface"),
    ("good", "surface"),
    ("warn", "surface"),
    ("bad", "surface"),
    ("good", "good-soft"),
    ("warn", "warn-soft"),
    ("bad", "bad-soft"),
]


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("fg,bg", TEXT_PAIRS)
def test_text_pairs_meet_wcag_aa(theme: str, fg: str, bg: str) -> None:
    tokens = _theme_blocks()[theme]
    ratio = _contrast(tokens[fg], tokens[bg])
    assert ratio >= AA_NORMAL, (
        f"{theme}: --{fg} ({tokens[fg]}) on --{bg} ({tokens[bg]}) "
        f"is {ratio:.2f}:1, below WCAG AA {AA_NORMAL}:1"
    )
