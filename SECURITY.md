# Security Policy

## Scope

This project is paper-trading first, but still processes trading logic and credentials in local configs.

## Reporting a vulnerability

Please do not open public issues for security vulnerabilities.

Report details privately with:

- vulnerable component/file
- reproduction steps
- impact assessment
- suggested mitigation (if available)

Maintainers will acknowledge receipt and provide status updates.

## Hardening guidance

- Keep `broker.paper_only = true` unless you explicitly control live execution.
- Avoid committing account IDs, API keys, or private gateway endpoints.
- Prefer localhost for broker gateways when possible.
