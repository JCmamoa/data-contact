# Security Policy

## Architectural Security Controls

- **Shift-Left Data Contracts:** Untrusted upstream records failing schema, type, nullability, range, or enum rules are blocked before reaching warehouse layers and routed to quarantine storage.
- **Governed Semantic Layer Allowlist:** Metric queries validate metric names, dimensions, and filter keys against `semantic_layer/metrics.yml` allowlists and use parameterized SQLite/PostgreSQL bindings to prevent SQL injection and metric drift.
- **Least-Privilege Cloud IaC:** AWS blueprint (`infra/aws/main.tf`) enforces KMS encryption, private S3 buckets, and scoped IAM roles; the Docker image runs as non-root `appuser`.

## Reporting a Vulnerability

Please open a private security advisory in GitHub (**Security → Advisories → Report a vulnerability**) with reproduction details.
