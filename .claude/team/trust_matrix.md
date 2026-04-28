# Trust Identity Matrix

All team members maintain a trust score for every other team member they interact with.

## Scale

| Score | Meaning |
|-------|---------|
| 1 | Very low trust — repeated failures, dishonesty, or poor quality |
| 2 | Low trust — notable issues, caution warranted |
| 3 | Neutral (default) — no strong signal either way |
| 4 | High trust — consistently reliable, good communication |
| 5 | Very high trust — exceptional reliability, goes above and beyond |

## Rules

- **Default:** Every pair starts at **3**.
- **Decreases:** Bad feelings, being misled/lied to, low-quality work product, broken commitments.
- **Increases:** Reliable delivery, honest communication, high-quality work, helpful collaboration.
- **Updates:** This file is updated whenever a trust-relevant interaction occurs. Changes should include a brief log entry explaining the adjustment.
- **Scope:** Trust is directional — A's trust in B may differ from B's trust in A.

## Matrix

Rows = the team member rating. Columns = the team member being rated. All cells default to **3** (neutral); first session has not occurred yet, so no adjustments have been recorded.

| Rater ↓ \ Rated → | Adaeze | Sayed | Bjørn | Camila | Petra | Fatima | Kalinda | Imelda | Yusuke | Léopold | Tomás |
|--------------------|--------|-------|-------|--------|-------|--------|---------|--------|--------|---------|-------|
| **Adaeze** (Manager) | — | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 |
| **Sayed** (Sys Architect) | 3 | — | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 |
| **Bjørn** (DevOps Architect) | 3 | 3 | — | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 |
| **Camila** (Data Lead) | 3 | 3 | 3 | — | 3 | 3 | 3 | 3 | 3 | 3 | 3 |
| **Petra** (Tech Lead) | 3 | 3 | 3 | 3 | — | 3 | 3 | 3 | 3 | 3 | 3 |
| **Fatima** (DevOps Eng) | 3 | 3 | 3 | 3 | 3 | — | 3 | 3 | 3 | 3 | 3 |
| **Kalinda** (Security) | 3 | 3 | 3 | 3 | 3 | 3 | — | 3 | 3 | 3 | 3 |
| **Imelda** (Data Eng) | 3 | 3 | 3 | 3 | 3 | 3 | 3 | — | 3 | 3 | 3 |
| **Yusuke** (Engineer P) | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | — | 3 | 3 |
| **Léopold** (Engineer S) | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | — | 3 |
| **Tomás** (QA Eng) | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | — |

## Adjustment Log

(Empty — no trust-relevant interactions recorded yet. First entries will be appended here as the team accumulates session history.)
