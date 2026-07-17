# ADR: Separate static dashboard markup from pipeline logic

## Status
Accepted.

## Context
The dashboard builder originally embedded the complete HTML, CSS, and JavaScript
document inside `_build_html()`. That made a data-packaging module more than
2,000 lines long and obscured the boundary between governed data preparation and
browser presentation.

The PDF uses executable ReportLab composition code. It remains in Python until
its report sections can be decomposed without changing pagination or visual output.

## Decision
- Store the dashboard document in `templates/executive_dashboard.html`.
- Keep three explicit build tokens: `__PLOTLY_CDN_URL__`, `__PLOTLY_SRI__`, and
  `__DATA_JSON__`.
- Render with UTF-8 file loading and deterministic string replacement. Do not
  add a template engine while the document has only these three substitutions.
- Test governance, accessibility, and interactions against the rendered HTML.

## Consequences
- `src/executive_dashboard.py` owns data shaping and publication; the template
  owns markup, styling, and client-side behavior.
- Dashboard output remains byte-identical for the same governed inputs.
- Frontend work no longer requires editing a Python string.
- The PDF composition module remains the largest maintenance hotspot and should
  be split by report section when a pagination-parity test is available.
