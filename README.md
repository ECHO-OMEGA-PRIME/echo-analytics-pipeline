> ## ✅ TESTED AND TRANSFERRED
>
> This repository has been consolidated into the canonical account. All code, fixes, tests, and
> documentation now live at:
>
> **→ https://github.com/echoomegaprime/echo-analytics-pipeline**
>
> - Destination commit: `00acc05d33b7d3a1387c3de99d51056f113b0390`
> - Cert Forge certificate: `cert_2e21de815f155d7a89b0bfc4cf134f69ae8f728e` — `PRODUCTION_READY`
>   (evidence Merkle root `e862d30c87fa558ffe219ca5c2b1c6012ebdee5c5e2a86d8c0a450a4ed70592f`,
>   verify at https://cert-api.echosforge.com/v1/certifications/cert_2e21de815f155d7a89b0bfc4cf134f69ae8f728e/verdict)
> - GitHub App Suite conformance: manual receipt at
>   [`.echo/repo-health.md`](https://github.com/echoomegaprime/echo-analytics-pipeline/blob/main/.echo/repo-health.md)
>   in the destination repo (GitHub App Suite auto-posting affected by build #29466 on this
>   account; this is the documented workaround)
> - Transfer date: 2026-08-12
>
> During transfer, two real fixes were made: a timing side-channel in the main API-key auth
> check was replaced with a constant-time comparison (fail-closed if unconfigured), and a fully
> implemented security-headers function that was never actually called anywhere was wired in —
> every response previously left this Worker with none of its declared security headers. See
> [SECURITY.md in the destination repo](https://github.com/echoomegaprime/echo-analytics-pipeline/blob/main/SECURITY.md).
>
> This legacy repository is preserved for provenance and is not actively maintained. Do not
> open issues or PRs here — use the destination repository above.

---

# echo-analytics-pipeline

> Business intelligence engine for Echo Omega Prime — aggregates fleet metrics, computes KPIs,
> detects anomalies, generates reports. See the destination repository linked above for current
> documentation, tests, and security fixes.
