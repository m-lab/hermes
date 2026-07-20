# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Report vulnerabilities privately via GitHub's
[**Report a vulnerability**](https://github.com/m-lab/hermes/security/advisories/new)
(Security → Advisories), or by email to **support@measurementlab.net**.

We aim to acknowledge reports within a few business days and will keep you
updated on remediation progress.

## Secrets and credentials

HERMES talks to external services and **must never contain committed secrets**:

- **Cloudflare Radar** — `CLOUDFLARE_API_TOKEN`, supplied via a local `.env`
  (which is git-ignored) or the runtime environment.
- **Google BigQuery** — Application Default Credentials
  (`gcloud auth application-default login`) or a service-account key mounted at
  runtime; **never** commit key files.

If you believe a credential has been committed, treat it as compromised:
rotate/revoke it immediately and notify the maintainers. GitHub **secret
scanning with push protection** is enabled on this repository to help catch
accidental commits before they land.
