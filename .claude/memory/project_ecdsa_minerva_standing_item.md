---
name: project_ecdsa_minerva_standing_item
description: ecdsa Minerva advisory (PYSEC-2026-1325) is pip-audit-ignored because RS256-only auth makes the ECDSA path unreachable; switching to ES* revokes the exemption.
metadata:
  type: project
---

`pip-audit --strict` ignores **PYSEC-2026-1325** (aliases **CVE-2024-23342**, **GHSA-wj6h-64fc-37mp**) — the python-ecdsa **Minerva timing attack on P-256**. Ignored in BOTH `ci.yml` (`security-audit`) and `.pre-commit-config.yaml` (pre-push `pip-audit`); they must stay verbatim-identical (local⇄CI parity, noorinalabs-main#684). Filed as ingest#128, 2026-07-10.

**There is no fix and none is coming** — the python-ecdsa project considers side-channel attacks out of scope. `ecdsa` cannot be dropped: `python-jose` pulls it, and `src/api/auth.py` genuinely uses `python-jose`.

**Why the exemption is sound:** the vulnerable primitive is **unreachable**, not merely unused in one direction. `src/api/auth.py:123,129` calls `jwt.decode(token, jwks, algorithms=["RS256"])` — RS256 is **RSA**. The advisory is a timing attack on **ECDSA P-256 scalar multiplication**, a code path this repo never enters. `ecdsa` is installed only because `python-jose` declares it as a dependency, not because anything calls it.

> Do not restate this as "we only verify, we don't sign." That weaker rationale is true but brittle — it would survive a switch to ES256 while the actual risk would not.

**What REVOKES this exemption:** admitting any EC algorithm (`ES256` / `ES384` / `ES512`) into that `algorithms=[...]` allowlist. The moment auth can verify an EC-signed token, the "unreachable primitive" argument dies and the ignore must be re-justified or removed. Revisit each wave.

**Two mechanics worth remembering (both verified, not assumed):**
- **`pip-audit --ignore-vuln` matches ALIASES.** Ignoring by `CVE-2024-23342` suppresses a finding reported as `PYSEC-2026-1325`, and vice versa. So an ignore list keyed on any one alias covers the others.
- Consequently **`noorinalabs-isnad-graph` was already covered** — its pre-existing `--ignore-vuln CVE-2024-23342` suppresses this advisory by alias, and it carries no `beautifulsoup4`/`soupsieve`. No change was needed there.

Same shape as the bleach standing item (`GHSA-g75f-g53v-794x`, noorinalabs-main#703): an unfixable upstream advisory, non-applicable to our usage, ignored with an inline rationale and a revocation condition. Related: [[project_data_pipeline_architecture]].
