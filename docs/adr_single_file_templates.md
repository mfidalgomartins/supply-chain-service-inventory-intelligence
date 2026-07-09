# ADR: Keep HTML/CSS/JS and PDF templates inline in `src/`

## Status
Accepted.

## Context
`src/executive_dashboard.py` (~1,970 lines) and `src/build_report.py`
(~1,930 lines) embed their entire output — HTML, CSS, JavaScript for the
dashboard; ReportLab flowables and prose for the PDF — as Python strings and
function calls in a single module each, rather than separate template files
under a `templates/` directory.

This was flagged in a portfolio audit as an architecture smell worth
reconsidering: splitting templates out is the conventional structure for a
codebase of this size, and would shorten both files considerably.

## Decision
Keep the single-file structure for now. Do not split templates out.

## Rationale
- **Determinism is the project's core guarantee.** The pipeline promises
  byte-identical regeneration of `index.html` and the PDF report across
  Python and NumPy versions (see the reproducibility notes in
  `src/executive_dashboard.py`). Introducing a template-loading layer (Jinja2
  or similar) adds another component whose own version and rendering
  behaviour would need the same reproducibility guarantees the rest of the
  pipeline already carries — a nontrivial extension of scope for a
  presentation-layer refactor.
- **The governance tests assert against generated output, not template
  source.** `tests/test_dashboard_governance.py` and
  `tests/test_dashboard_contrast.py` parse the *rendered* HTML string
  returned by `_build_html()`. A template split would require rewriting
  these tests' extraction logic and re-verifying every assertion still
  holds, with real risk of silently loosening a check during the move.
  This is not free.
- **The single-file approach has no code-quality cost today.** Both modules
  are already covered by the 95% coverage floor, pass mypy and ruff cleanly,
  and are logically organised internally (data preparation, then styling
  constants, then template assembly, in a consistent order in both files).
  The "monolithic" flag is about line count and convention, not measured
  defects.
- **The realistic effort (4-6 hours) buys marginal benefit relative to other
  work.** A full split with byte-identical-output verification before/after
  is a real, scoped task — better done deliberately, on its own, than folded
  into an unrelated change.

## Revisit When
- A second output format is added that would genuinely share template
  fragments with the dashboard or report (at that point, duplication becomes
  the cost, not line count).
- Either file grows enough that locating a specific section becomes a real
  friction point in practice, not just a line-count heuristic.
