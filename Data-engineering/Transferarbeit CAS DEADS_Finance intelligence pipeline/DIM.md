## `team.dim.umccseasoncycle` — Dimension Table (SQL, static)

**Purpose:** maps every UMCC football season to its 3-season reporting cycle.

- Generated via a one-time `CREATE OR REFRESH TABLE ... AS` SQL statement, not a pipeline table
- Logic: cycles run in consecutive 3-season groups, anchored backward from the most recent complete cycle (`24-25, 25-26, 26-27` → `24-27`), extending back to season `90-91`
- Static reference data — not expected to change on a regular cadence; lives in the shared `team.dim` schema alongside other project dimension tables

```sql
CREATE OR REPLACE TABLE team.dim.UMCCseasoncycle AS
WITH years AS (
  SELECT explode(sequence(1990, 2026)) AS start_year
),
calc AS (
  SELECT
    start_year,
    concat(
      lpad(CAST(start_year % 100 AS STRING), 2, '0'), '-',
      lpad(CAST((start_year + 1) % 100 AS STRING), 2, '0')
    ) AS season,
    2026 - start_year AS n
  FROM years
),
grouped AS (
  SELECT
    season,
    start_year,
    floor(n / 3) AS grp
  FROM calc
)
SELECT
  season,
  concat(
    lpad(CAST((2024 - 3 * grp) % 100 AS STRING), 2, '0'), '-',
    lpad(CAST((2027 - 3 * grp) % 100 AS STRING), 2, '0')
  ) AS cycle
FROM grouped
ORDER BY start_year;

```
---
