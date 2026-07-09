# Security Policy

## Scope

This repository is a static, offline analytics pipeline: it generates a
synthetic dataset, runs local SQL/Python transformations, and publishes a
static dashboard (`index.html`), static charts, and a PDF report. It has no
server component, no user-facing input, and no production credentials or
secrets. The main supply-chain-relevant surface is the pinned Plotly bundle
the dashboard loads from a CDN, which is verified at load time with a
Subresource Integrity (SHA-384) hash.

## Reporting a Vulnerability

If you find a security issue — a dependency vulnerability, a Subresource
Integrity bypass, or anything else with a plausible security impact — please
report it privately by emailing **mfidalgomartins@gmail.com** rather than
opening a public issue. Include:

- A description of the issue and its potential impact.
- Steps to reproduce, if applicable.
- The affected file(s) or dependency version(s).

You can expect an initial response within a few days. There is no bug-bounty
program; this is a personal portfolio project maintained on a best-effort
basis.

## Supported Versions

Only the latest tagged release and the `main` branch are supported. Older
tags are kept for historical reference only.

## Dependency Auditing

Runtime and development dependencies are scanned with [`pip-audit`](https://pypi.org/project/pip-audit/)
on every CI run (`.github/workflows/analytics-ci.yml`), and Dependabot keeps
version pins current (`.github/dependabot.yml`).
