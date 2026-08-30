# DCB Pipeline — Technical Documentation

Lakeflow Declarative Pipeline covering Landing → Bronze → Silver → Gold for the DCB dataset.

---

## Architecture Overview

- **Catalog:** `team`
- **Landing volume:** `team.landing.dcb` (subfolders: `historical/`, `incremental/`)
- **Bronze schema:** `team.bronze`
- **Silver schema:** `team.silver`
- **Gold schema:** `team.gold`
- **Orchestration:** single Lakeflow Declarative Pipeline (Python, `pyspark.pipelines` / `dp` API), scheduled weekly for **Friday 13:00 (Swiss time)**. New source files are expected to be uploaded to the landing volume each Friday morning, ahead of that run, so a given week's upload is picked up the same day. Full DAG (`dcb_raw` → `dcb` → `team.silver.dcb` → `team.gold.dcb`) executes on every run, with streaming/materialized-view engines each determining incremental vs. full recompute internally.

---

## I) Original data storage, ingestion (LANDING → BRONZE)

### A) Stage 1 - `dcb_raw` (Streaming table)

**Purpose:** structural extraction from raw Excel files in landing, no business logic.

**IMPORTANT:** in Databricks, both stages are computed in one python file. They're split out in github for simpler documentation


- Source: Auto Loader (`cloudFiles`) streaming read from `/Volumes/team/landing/dcb/`, recursively covering both `historical/` and `incremental/` subfolders in a single stream
- Format: native Databricks Excel reader (`cloudFiles.format = "excel"`, Beta feature, requires DBR 17.1+)
- Sheet/range targeting: `dataAddress = "Summary!B5:Q100"` — extracts only the `Summary` tab, skips a hidden row and the label column, bounded to the actual data footprint
- `headerRows = 0` — header row is not trusted from the source file (inconsistent labeling across files, e.g. `TEAM code` vs `TEAM's code` vs double-spaced variants); columns are instead renamed **positionally** via `.toDF(*canonical_names)` against a manually confirmed, fixed 16-column list
- `cloudFiles.schemaEvolutionMode = "none"` — schema is fixed by design; Excel streaming does not support schema evolution
- `cloudFiles.schemaLocation` — persisted Auto Loader schema cache; must be manually cleared (along with the checkpoint) whenever the extraction range or column count changes
- Metadata columns added: `_ingested_at`, `_source_file` (raw `_metadata.file_path`, URL-encoded)
- **`_source_file_modified_at`** — cloud storage last-modified time, captured immediately after `_source_file`. This is the timestamp `dcb` (stage 2) uses to determine which uploaded file is authoritative for a given competition/season. It's a property of the file itself in storage, not of when the pipeline happens to run, so it stays correct however irregularly the pipeline is triggered relative to when files are actually uploaded
- `_source_file_decoded` — `%20` sequences replaced with literal spaces, since `_metadata.file_path` returns URL-encoded paths, which silently broke filename-based regex extraction
- Filename-derived columns extracted via regex against the decoded path, based on a fixed filename taxonomy (`YYYY-MM-DD <COMPETITION> DCB <SEASON>`):
  - `document_date` (cast to `date`)
  - `competition`
  - `season`
- **Data quality (warn-only expectations):**
  - `valid_season`: `season != ''`
  - `valid_competition`: `competition != ''`
  - `valid_document_date`: `document_date IS NOT NULL`
  - Note: `regexp_extract` returns `''` (not `NULL`) on non-match, so expectations check against empty string, not null

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, dense_rank, monotonically_increasing_id,
    last, regexp_extract, regexp_replace, to_date,
    row_number, concat_ws, lit
)
from pyspark.sql import Window

filename_pattern = r"(\d{4}-\d{2}-\d{2}) (\w+) DCB (\d{2}-\d{2})"

canonical_names = [
    "Budget_category",
    "UEFA_code",
    "TEAM_code",
    "Budget_line",
    "Responsible",
    "Updated",
    "Initial_total_budget_EUR",
    "Total_budget_Forecast_1_EUR",
    "Total_budget_Forecast_2_EUR",
    "Total_budget_Forecast_3_EUR",
    "Total_budget_Forecast_4_EUR",
    "Total_budget_last_forecast_EUR",
    "Total_amount_spent_EUR",
    "Total_expected_spend_EUR",
    "Total_remaining_Budget_EUR",
    "Notes",
]

numeric_columns = [
    "Initial_total_budget_EUR",
    "Total_budget_Forecast_1_EUR",
    "Total_budget_Forecast_2_EUR",
    "Total_budget_Forecast_3_EUR",
    "Total_budget_Forecast_4_EUR",
    "Total_budget_last_forecast_EUR",
    "Total_amount_spent_EUR",
    "Total_expected_spend_EUR",
    "Total_remaining_Budget_EUR",
]


@dp.table(
    name="dcb_raw",
    comment=(
        "DCB Summary tab, raw extraction from landing (pre-forward-fill), "
        "columns renamed positionally to canonical names, with filename "
        "metadata extracted from decoded source path. ADDING INCREMENTAL "
        "DOWNLOAD CHECK: carries _source_file_modified_at (cloud storage "
        "last-modified time) so dcb can determine which uploaded file is "
        "current per competition/season."
    ),
    table_properties={"quality": "bronze"}
)
@dp.expect("valid_season", "season != ''")
@dp.expect("valid_competition", "competition != ''")
@dp.expect("valid_document_date", "document_date IS NOT NULL")
def dcb_raw():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "excel")
        .option("dataAddress", "Summary!B5:Q100")
        .option("headerRows", 0)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/team/landing/dcb/_schema/")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/team/landing/dcb/")
    )
    df = df.toDF(*canonical_names)

    df = df.withColumn("_ingested_at", current_timestamp())
    df = df.withColumn("_source_file", col("_metadata.file_path"))
    
    # INCREMENTAL DOWNLOAD CHECK
    # Cloud storage last-modified time - the robust "which upload is
    # current" signal, independent of document_date (which is parsed from
    # the filename and only has day-level precision, so it can't
    # distinguish two files uploaded for the same season on the same day).
    df = df.withColumn("_source_file_modified_at", col("_metadata.file_modification_time"))
    # =======================================================================
    df = df.withColumn("_source_file_decoded", regexp_replace(col("_source_file"), "%20", " "))

    df = df.withColumn(
        "document_date",
        to_date(regexp_extract(col("_source_file_decoded"), filename_pattern, 1), "yyyy-MM-dd")
    )
    df = df.withColumn("competition", regexp_extract(col("_source_file_decoded"), filename_pattern, 2))
    df = df.withColumn("season", regexp_extract(col("_source_file_decoded"), filename_pattern, 3))

    return df

```

---

### Stage 2 - `dcb` (Materialized view)

**Purpose:** select the single authoritative file per (competition, season) and perform structural cleanup of Excel-specific artifacts; still no business logic beyond file selection.

- Reads `dcb_raw` as a **batch** source (`spark.read.table`) — required because the forward-fill logic below uses an order-dependent window function, which is unsupported on streaming DataFrames
- **Latest-file filter (runs first, before anything else):** rows are restricted to the single most recently modified source file per `(competition, season)`, ranked by `_source_file_modified_at` via `dense_rank()` partitioned on `(competition, season)` and ordered descending. `dense_rank()` rather than `row_number()` is required here — every row belonging to one file shares the same `_source_file_modified_at`, so `dense_rank()` correctly keeps *all* of that file's rows, where `row_number()` would arbitrarily cut the winning file off partway through. Superseded uploads for a season remain in `dcb_raw` as history but are excluded from `dcb` onward — a corrected re-upload replaces the prior file's rows here rather than being appended alongside them, regardless of whether the old file is deleted from the landing volume
- `_row_id` generated via `monotonically_increasing_id()` here (moved out of the streaming stage after hitting `Expression(s): monotonically_increasing_id() is not supported with streaming DataFrames/Datasets`), applied after the latest-file filter so IDs are only ever assigned to rows that survive it
- **Row filtering:** rows dropped where `UEFA_code`, `TEAM_code`, or `Budget_line` is null — removes trailing blank rows within the extraction range and rows without a valid identifying key
- **Merged-cell forward-fill:** `Budget_category` (a merged Excel column, populated only on the first row of each merge group) is forward-filled using a window function (`last(..., ignorenulls=True)`), partitioned by `_source_file` and ordered by `_row_id`, so fill never bleeds across files
- **Type correction:** all budget/amount columns explicitly cast to `double` (source inference defaulted to string, only fully resolved after fixing the row-offset issue)
- **Row ID generation:** composite, human-readable key — `row_id = concat_ws("_", "dcb", competition, season, row_number)` — replacing the initial `monotonically_increasing_id()` long value, using `row_number()` per `_source_file` for a clean sequential counter. This key is now guaranteed unique across the table's full history: before the latest-file filter existed, two uploads for the same `(competition, season)` could produce colliding `row_id` values since the key doesn't include a per-file component; the filter above ensures only one source file per `(competition, season)` ever reaches this step, so the collision risk no longer applies

```python

@dp.table(
    name="dcb",
    comment=(
        "DCB Summary tab: filtered to complete rows (UEFA_code, TEAM_code, "
        "Budget_line), Budget_category forward-filled, numeric columns "
        "cast, row_id generated. ADDING INCREMENTAL DOWNLOAD CHECK: also "
        "filtered to the most recently modified file per (competition, "
        "season) - superseded uploads remain in dcb_raw as history but are "
        "excluded here."
    ),
    table_properties={"quality": "bronze"}
)
def dcb():
    df = spark.read.table("dcb_raw")

    # INCREMENTAL DOWNLOAD CHECK =================
    # Only trust the most recently modified file per (competition, season).
    # dense_rank (not row_number) is required: every row from one file
    # shares the same _source_file_modified_at, so dense_rank correctly
    # keeps ALL of that file's rows, where row_number would arbitrarily
    # cut it off partway through. This also removes the row_id collision
    # risk that existed before this filter, since only one file per season
    # now survives to reach concat_ws("_", "dcb", competition, season,
    # row_number) below.
    latest_window = Window.partitionBy("competition", "season").orderBy(
        col("_source_file_modified_at").desc()
    )
    df = (
        df.withColumn("_snapshot_rank", dense_rank().over(latest_window))
        .filter(col("_snapshot_rank") == 1)
        .drop("_snapshot_rank")
    )
    # =======================================================================

    df = df.withColumn("_row_id", monotonically_increasing_id())

    df = df.filter(
        col("UEFA_code").isNotNull() &
        col("TEAM_code").isNotNull() &
        col("Budget_line").isNotNull()
    )

    w = (Window
         .partitionBy("_source_file")
         .orderBy("_row_id")
         .rowsBetween(Window.unboundedPreceding, 0))

    df = df.withColumn(
        "Budget_category",
        last(col("Budget_category"), ignorenulls=True).over(w)
    )

    for c in numeric_columns:
        df = df.withColumn(c, col(c).cast("double"))

    row_order_window = Window.partitionBy("_source_file").orderBy("_row_id")
    df = df.withColumn("_row_number", row_number().over(row_order_window))
    df = df.withColumn(
        "row_id",
        concat_ws("_", lit("dcb"), col("competition"), col("season"), col("_row_number"))
    )

    return df
```

---

## II) Data standardisation (BRONZE → SILVER) `team.silver.dcb` (Materialized View)


**Purpose:** business-relevant column selection and dimensional enrichment.

- Reads `team.bronze.dcb` (fully qualified — cross-schema read within the same pipeline; the `schema=` decorator parameter was attempted first but is not supported in this pipeline runtime, so the table is targeted via fully qualified `name="team.silver.dcb"` instead)
- Column selection limited to the 5 business-relevant budget fields:
  - `Budget_category`, `Budget_line`, `Initial_total_budget_EUR`, `Total_budget_last_forecast_EUR`, `Total_expected_spend_EUR`
- Lineage/traceability columns retained: `season`, `competition`, `document_date`, `_source_file`, `_ingested_at`, `row_id`
- **Dimensional enrichment:** left join to `team.dim.umccseasoncycle` on `season`, adding `cycle`
  - Left join (not inner) chosen deliberately so a DCB row is never silently dropped if a season is missing from the DIM table
- **Data quality (warn-only expectation):** `valid_cycle`: `cycle IS NOT NULL`

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="team.silver.dcb",
    comment="DCB harmonized to relevant budget columns, enriched with season-cycle mapping from team.dim.seasoncycle.",
    table_properties={"quality": "silver"}
)
@dp.expect("valid_cycle", "cycle IS NOT NULL")
def dcb_silver():
    bronze = spark.read.table("team.bronze.dcb")
    dim = spark.read.table("team.dim.UMCCseasoncycle")

    df = bronze.select(
        "Budget_category",
        "Budget_line",
        "Initial_total_budget_EUR",
        "Total_budget_last_forecast_EUR",
        "Total_expected_spend_EUR",
        "season",
        "competition",
        "document_date",
        "_source_file",
        "_ingested_at",
        "row_id",
    )

    return df.join(dim, on="season", how="left")
```

## III) Business readiness (SILVER → GOLD) `team.gold.dcb` (Materialized View)

**Purpose:** analytics-ready KPI layer.

- Reads `team.silver.dcb`
- **`budget_accuracy_calc`**: `(Total_expected_spend_EUR / Total_budget_last_forecast_EUR) * 100`, rounded to 2 decimals — continuous metric, calculated for every row
- **`budget_accuracy_sum`**: categorical label derived from the same ratio (pre-scaling), using closed, non-overlapping bucket boundaries:
  - `> 1.15` → *significantly overspent*
  - `> 1.05` and `≤ 1.15` → *overspent*
  - `≥ 0.95` and `≤ 1.05` → *in budget*
  - `≥ 0.85` and `< 0.95` → *underspent*
  - `< 0.85` → *significantly underspent*
- **`Budget_difference`**: `Total_expected_spend_EUR − Total_budget_last_forecast_EUR`
- **`Cost_main_category`**: static literal `"Marketing costs"` on every row (placeholder pending a future proper category mapping)
- **Data quality (warn-only expectation):** `valid_budget_ratio`: `Total_budget_last_forecast_EUR IS NOT NULL AND != 0` — guards the division underlying the two accuracy columns

```python

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, round as spark_round, lit

@dp.table(
    name="team.gold.dcb",
    comment="DCB gold: budget accuracy metrics, budget difference, and cost category tagging.",
    table_properties={"quality": "gold"}
)
@dp.expect("valid_budget_ratio", "Total_budget_last_forecast_EUR IS NOT NULL AND Total_budget_last_forecast_EUR != 0")
def dcb_gold():
    df = spark.read.table("team.silver.dcb")

    ratio = col("Total_expected_spend_EUR") / col("Total_budget_last_forecast_EUR")

    df = df.withColumn("budget_accuracy_calc", spark_round(ratio * 100, 2))

    df = df.withColumn(
        "budget_accuracy_sum",
        when(ratio > 1.15, "significantly overspent")
        .when((ratio > 1.05) & (ratio <= 1.15), "overspent")
        .when((ratio >= 0.95) & (ratio <= 1.05), "in budget")
        .when((ratio >= 0.85) & (ratio < 0.95), "underspent")
        .when(ratio < 0.85, "significantly underspent")
        .otherwise(None)
    )

    df = df.withColumn(
        "Budget_difference",
        col("Total_expected_spend_EUR") - col("Total_budget_last_forecast_EUR")
    )

    df = df.withColumn("Cost_main_category", lit("Marketing costs"))

    return df
```

---

## Support / Pipeline maintenance execution code

### CLEAN

#### Reset Auto Loader schema and checkpoint state (Python, run in a notebook attached to a cluster):
Clears the persisted Auto Loader schema inference cache and streaming checkpoint for the dcb landing volume. Required whenever the source extraction logic changes structurally (e.g. cell range, header handling, or column count) — since cloudFiles.schemaEvolutionMode = "none" locks the schema to whatever was inferred on the first run, this cache must be manually cleared before Auto Loader will pick up the new structure.

```python
dbutils.fs.rm("/Volumes/team/landing/dcb/_schema/", recurse=True)
dbutils.fs.rm("/Volumes/team/landing/dcb/_checkpoints/", recurse=True)
```

#### Drop bronze DCB tables (SQL, run in the SQL Editor):
Drops the existing bronze tables so they can be fully rebuilt from a clean state. Run alongside the schema/checkpoint reset above whenever dcb_raw's or dcb's structure changes — without this, the pipeline would attempt to write a new schema into a table still expecting the old one, causing a mismatch error.

```sql
DROP TABLE IF EXISTS team.bronze.dcb_raw;
DROP TABLE IF EXISTS team.bronze.dcb;
```

---

### CHECKS

#### Per-file ingestion sense check:
Shows every distinct source file ingested into dcb_raw so far, with row count and ingestion timestamp, ordered by most recently ingested first. Useful after uploading a new file and running the pipeline — confirms exactly which file was picked up and how many rows it contributed, without digging through the pipeline UI or event log.

```sql
SELECT
    _source_file,
    COUNT(*) AS row_count,
    MAX(_ingested_at) AS ingested_at
FROM team.bronze.dcb_raw
GROUP BY _source_file
ORDER BY ingested_at DESC;
```

#### Row-count consistency check (raw vs. filtered):
Compares the total unfiltered row count in dcb_raw against the filtered, forward-filled row count in dcb. Since dcb drops incomplete rows (missing UEFA_code, TEAM_code, or Budget_line), the filtered total should always be less than or equal to the raw total — a quick way to confirm the filtering logic is behaving as expected as new files are added.

```sql
SELECT COUNT(*) FROM team.bronze.dcb_raw;   -- unfiltered total
SELECT COUNT(*) FROM team.bronze.dcb;      
```
