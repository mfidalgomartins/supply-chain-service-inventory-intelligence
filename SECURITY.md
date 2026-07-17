# Security Policy

## Scope

This repository is a local analytics pipeline with no server component,
authentication flow, or production credentials. It can generate synthetic
data or ingest configured CSV/Parquet exports from canonical directories and
split ERP/WMS sources. External files and their text fields are therefore
untrusted inputs.

The pipeline validates schemas, domains, references, business keys, and file
hashes before publication. Source readiness checks freshness and breaking
schema drift before replacing canonical data. Dashboard JSON escapes characters
that could terminate its embedded script element. The static dashboard loads a pinned
Plotly bundle from a CDN and verifies it with a Subresource Integrity
(SHA-384) hash.

Object publication is local by default. The optional S3-compatible backend uses
the standard AWS credential chain, conditional immutable writes, SHA-256
checksums, and configured server-side encryption. Bucket credentials are never
accepted in `configs/pipeline.json` and are not emitted in run telemetry.

Do not place credentials, personal data, or commercially sensitive extracts in
the repository. Keep source directories and object prefixes access-controlled,
use least-privilege write access, enable bucket versioning for production, and
review generated publications before sharing them.

## Reporting a Vulnerability

If you find a security issue — a dependency vulnerability, a Subresource
Integrity bypass, or anything else with a plausible security impact — please
report it privately by emailing **mfidalgomartins@gmail.com**. Do not open a
public issue for an undisclosed vulnerability. Include:

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
