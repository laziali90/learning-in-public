# Pipeline Overview

## I) Data Ingestion and Readability (BRONZE, BRONZE → SILVER)

### A) Raw Data Ingestion and Storage

All relevant tables have been uploaded as CSV tables via Databricks table ingestion, changed into Delta tables. This is to simulate a data load from a server or connector, which would be the usual way to ingest data on the landing zone. This solution has been selected because of the explorative nature of this workflow — TEAM would require a Cloud based container / storage.

**Schema:** `team > landing_bronze`

- `2526_ucl_audience_byday`
- `2526_ucl_audience_byposttype`
- `2526_ucl_valuation_memberposts`
- `2526_ucl_valuation_uniquememberposts`
- `2526_uecl_audience_byday`
- `2526_uecl_audience_byposttype`
- `2526_uel_audience_byday`
- `2526_uel_audience_byposttype`
- `2526_ueluecl_valuation_memberposts`

**Data / table storage:** Databricks Free edition serverless SQL server

---

### B) Rules Mapping and Transfer to Silver

**Notebook:** `TEAM > Social data_1_Rules mapping datasets and transfer to silver`

Scans the bronze schema, matches each table to its ruleset based on a stable name pattern, cleans column names and data, and transfers data from bronze to silver. Audience tables are copied as-is; valuation tables use an upsert CDC approach via `SOCIALPOST_ID`.

---

#### 1.0 Config

Sets the catalog and schema names used throughout the notebook.

```python
CATALOG = "team"
BRONZE_SCHEMA = "landing_bronze"
SILVER_SCHEMA = "silver"
```

---

#### 1.1 Registry

Defines the stable name patterns used to match bronze tables to their ruleset. Add one entry per dataset type. The pattern should exclude date stamps and year codes that change between uploads (e.g. `2526_`) — only the stable core name is used.

```python
RULES_REGISTRY = [
    {"name": "audience_byposttype",         "pattern": "audience_byposttype",         "match_mode": "contains"},
    {"name": "audience_byday",              "pattern": "audience_byday",              "match_mode": "contains"},
    {"name": "valuation_memberposts",       "pattern": "valuation_memberposts",       "match_mode": "contains"},
    {"name": "valuation_uniquememberposts", "pattern": "valuation_uniquememberposts", "match_mode": "contains"},
]
```

---

#### 1.2 Matching to Ruleset

Lists all tables in the bronze schema and matches each one against the registry. Tables that don't match any pattern land in `unmatched_tables` and are not processed.

```python
import re

def list_bronze_tables(catalog, schema):
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    return [r["tableName"] for r in rows]

def match_table_to_rules(table_name, registry):
    for entry in registry:
        if entry["match_mode"] == "contains":
            if entry["pattern"] in table_name:
                return entry
        elif entry["match_mode"] == "regex":
            if re.search(entry["pattern"], table_name):
                return entry
        else:
            raise ValueError(f"Unknown match_mode '{entry['match_mode']}' in registry entry '{entry['name']}'")
    return None

bronze_tables = list_bronze_tables(CATALOG, BRONZE_SCHEMA)

matched = []
unmatched_tables = []

for t in bronze_tables:
    entry = match_table_to_rules(t, RULES_REGISTRY)
    if entry is not None:
        matched.append((t, entry))
    else:
        unmatched_tables.append(t)
```

---

#### 1.3 Results of Matching

Prints the matching results for verification. Any discrepancy between bronze tables found and matched tables means a dataset has not been named correctly and needs to be renamed before re-running.

```python
print(f"Bronze tables found: {len(bronze_tables)}")
print(f"Matched to a ruleset: {len(matched)}")
for t, entry in matched:
    print(f"  {t}  ->  ruleset '{entry['name']}'")

if unmatched_tables:
    print(f"\nWARNING - {len(unmatched_tables)} table(s) matched NO ruleset (not processed):")
    for t in unmatched_tables:
        print(f"  {t}")
```

**Results (01/07/2026):**

**Bronze tables found:** 10
**Matched to a ruleset:** 10

| Table | Ruleset |
|---|---|
| `2526_ucl_audience_byday` | audience_byday |
| `2526_ucl_audience_byposttype` | audience_byposttype |
| `2526_ucl_valuation_memberposts` | valuation_memberposts |
| `2526_ucl_valuation_uniquememberposts` | valuation_uniquememberposts |
| `2526_uecl_audience_byday` | audience_byday |
| `2526_uecl_audience_byposttype` | audience_byposttype |
| `2526_uel_audience_byday` | audience_byday |
| `2526_uel_audience_byposttype` | audience_byposttype |
| `2526_ueluecl_valuation_memberposts` | valuation_memberposts |
| `2526_ueluecl_valuation_uniquememberposts` | valuation_uniquememberposts |

---

#### 2.0 Transfer / Connector to Silver

Transfers matched tables from bronze to silver with the following cleaning steps applied:

**Column name cleaning** — Delta tables don't accept special characters in column names. The following transformations are applied:
- `€` is replaced with `EUR`
- All remaining special characters (spaces, brackets, slashes etc.) are replaced with underscores

**Excel error cleaning** — Excel error values (`#N/A`, `#DIV/0!`, `#VALUE!`, `#REF!`, `#NAME?`, `#NULL!`, `#NUM!`) are replaced with `NULL`.

**DIM column lowercasing** — the following columns are lowercased to ensure case-insensitive matching in quality checks: `PLATFORM`, `SEASON`, `STAGE`, `MONTH`, `GS_KO`, `COVID_DATES`, `GROUPTYPE`, `WEEKDAY`.

**Transfer logic:**
- **Audience tables** — full overwrite on every run (`mode = overwrite`)
- **Valuation tables** — upsert CDC via `SOCIALPOST_ID`: existing rows are updated with the latest data (allowing corrections to flow through), new rows are inserted. On the first run a full load is performed.

```python
import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F

VALUATION_PATTERN = "valuation"

EXCEL_ERRORS = ["#N/A", "ERROR:#N/A", "#DIV/0!", "ERROR:#DIV/0!",
                "#VALUE!", "ERROR:#VALUE!", "#REF!", "ERROR:#REF!",
                "#NAME?", "ERROR:#NAME?", "#NULL!", "ERROR:#NULL!",
                "#NUM!", "ERROR:#NUM!"]

DIM_STRING_COLUMNS = [
    "PLATFORM", "SEASON", "STAGE", "MONTH", "GS_KO",
    "COVID_DATES", "GROUPTYPE", "WEEKDAY"
]

for table_name, entry in matched:
    bronze_path = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    silver_path = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    df = spark.table(bronze_path)
    df = df.toDF(*[re.sub(r'[^a-zA-Z0-9_]', '_', c.replace('€', 'EUR')) for c in df.columns])

    for error_val in EXCEL_ERRORS:
        df = df.replace(error_val, None)

    for col_name in DIM_STRING_COLUMNS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.lower(F.col(col_name)))

    if VALUATION_PATTERN in table_name:
        df_incoming = df.withColumn("_ingested_at", F.current_timestamp())

        if not spark.catalog.tableExists(silver_path):
            df_incoming.write.mode("overwrite").saveAsTable(silver_path)
            print(f"Full load (first time): {bronze_path} -> {silver_path}")
        else:
            silver_table = DeltaTable.forName(spark, silver_path)
            silver_table.alias("silver").merge(
                df_incoming.alias("incoming"),
                "silver.SOCIALPOST_ID = incoming.SOCIALPOST_ID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"Upsert load: {bronze_path} -> {silver_path}")
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_path)
        print(f"Raw copy: {bronze_path} -> {silver_path}")
```

---

#### 2.1 Run Summary

```python
summary = []
for table_name, entry in matched:
    silver_path = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    try:
        count = spark.table(silver_path).count()
        status = "ok"
    except Exception as e:
        count = None
        status = f"ERROR: {str(e)[:80]}"

    summary.append({
        "table": table_name,
        "ruleset": entry["name"],
        "type": "valuation (upsert CDC)" if VALUATION_PATTERN in table_name else "audience (raw copy)",
        "silver_row_count": count,
        "silver_table": silver_path,
        "status": status,
    })

display(spark.createDataFrame(summary))
```

**Results (01/07/2026):**

| Table | Ruleset | Type | Silver Row Count | Status |
|---|---|---|---|---|
| `2526_ucl_audience_byday` | audience_byday | audience (raw copy) | — | ok |
| `2526_ucl_audience_byposttype` | audience_byposttype | audience (raw copy) | — | ok |
| `2526_ucl_valuation_memberposts` | valuation_memberposts | valuation (upsert CDC) | 593,975 | ok |
| `2526_ucl_valuation_uniquememberposts` | valuation_uniquememberposts | valuation (upsert CDC) | 189,940 | ok |
| `2526_uecl_audience_byday` | audience_byday | audience (raw copy) | 21,042 | ok |
| `2526_uecl_audience_byposttype` | audience_byposttype | audience (raw copy) | 22,992 | ok |
| `2526_uel_audience_byday` | audience_byday | audience (raw copy) | 45,863 | ok |
| `2526_uel_audience_byposttype` | audience_byposttype | audience (raw copy) | 37,667 | ok |
| `2526_ueluecl_valuation_memberposts` | valuation_memberposts | valuation (upsert CDC) | 243,683 | ok |
| `2526_ueluecl_valuation_uniquememberposts` | valuation_uniquememberposts | valuation (upsert CDC) | 87,784 | ok |

---

#### 2.2 Column Name Changes from Bronze to Silver

```python
for table_name, entry in matched:
    bronze_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    silver_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")

    changes = [
        (b, s) for b, s in zip(bronze_df.columns, silver_df.columns) if b != s
    ]

    if changes:
        print(f"\n{table_name}:")
        for bronze_col, silver_col in changes:
            print(f"  {bronze_col}  ->  {silver_col}")
```

**Results (01/07/2026):**

*2526_ucl_audience_byday*
| Bronze | Silver |
|---|---|
| `CUSTOM DATE` | `CUSTOM_DATE` |
| `GS/KO` | `GS_KO` |
| `COVID DATES` | `COVID_DATES` |
| `COVID 2019` | `COVID_2019` |
| `IG Total Album Views` | `IG_Total_Album_Views` |
| `IG ALBUM FIRST TILE VIEWS` | `IG_ALBUM_FIRST_TILE_VIEWS` |

*2526_ucl_audience_byposttype*
| Bronze | Silver |
|---|---|
| `GS/KO` | `GS_KO` |

*2526_ucl_valuation_memberposts*
| Bronze | Silver |
|---|---|
| `OVERALL PARTNER` | `OVERALL_PARTNER` |
| `POST TIME` | `POST_TIME` |
| `WEEK COMMENCING` | `WEEK_COMMENCING` |
| `Collab Post` | `Collab_Post` |
| `MAX VALUE PEAK INT VAL (€)` | `MAX_VALUE_PEAK_INT_VAL__EUR_` |
| `VPL - ADJ` | `VPL___ADJ` |
| `VPC  - ADJ` | `VPC____ADJ` |
| `VPS  - ADJ` | `VPS____ADJ` |
| `VPV  - ADJ` | `VPV____ADJ` |
| `ADVALUE_LIKES - ADJ` | `ADVALUE_LIKES___ADJ` |
| `ADVALUE_COMMENTS - ADJ` | `ADVALUE_COMMENTS___ADJ` |
| `ADVALUE_SHARES - ADJ` | `ADVALUE_SHARES___ADJ` |
| `ADVALUE_VIEWS - ADJ` | `ADVALUE_VIEWS___ADJ` |
| `MAX VALUE PEAK INT VAL (€) - ADJ` | `MAX_VALUE_PEAK_INT_VAL__EUR____ADJ` |
| `CPE LOOKUP PLATFORM POST TYPE` | `CPE_LOOKUP_PLATFORM_POST_TYPE` |
| `FIXED PROMO TYPE` | `FIXED_PROMO_TYPE` |
| `FIXED PROMO SCORE` | `FIXED_PROMO_SCORE` |
| `Media Value (Level 1)_Current` | `Media_Value__Level_1__Current` |
| `Media Value (Level 6)_Current` | `Media_Value__Level_6__Current` |
| `Media Value (Level 2)_New` | `Media_Value__Level_2__New` |
| `Media Value (Level 3)_New` | `Media_Value__Level_3__New` |
| `Media Value (Level 4)_New` | `Media_Value__Level_4__New` |
| `Media Value (Level 5)_New` | `Media_Value__Level_5__New` |
| `Media Value (Level 4)_New - Hookit Original Value for Carousel` | `Media_Value__Level_4__New___Hookit_Original_Value_for_Carousel` |
| `Album PQ Applied` | `Album_PQ_Applied` |
| `Orignal views ` | `Orignal_views_` |

*2526_ucl_valuation_uniquememberposts*
| Bronze | Silver |
|---|---|
| `POST TIME` | `POST_TIME` |
| `WEEK COMMENCING` | `WEEK_COMMENCING` |
| `Orignal views ` | `Orignal_views_` |

*2526_uecl_audience_byday*
| Bronze | Silver |
|---|---|
| `GS/KO` | `GS_KO` |
| `COVID DATES` | `COVID_DATES` |
| `COVID 2019` | `COVID_2019` |

*2526_uecl_audience_byposttype*
| Bronze | Silver |
|---|---|
| `GS/KO` | `GS_KO` |
| `COVID DATES` | `COVID_DATES` |

*2526_uel_audience_byday*
| Bronze | Silver |
|---|---|
| `GS/KO` | `GS_KO` |
| `COVID DATES` | `COVID_DATES` |
| `COVID 2019` | `COVID_2019` |
| `IG Total Album Views` | `IG_Total_Album_Views` |

*2526_uel_audience_byposttype*
| Bronze | Silver |
|---|---|
| `GS/KO` | `GS_KO` |

*2526_ueluecl_valuation_memberposts*
| Bronze | Silver |
|---|---|
| `OVERALL PARTNER` | `OVERALL_PARTNER` |
| `POST TIME` | `POST_TIME` |
| `WEEK COMMENCING` | `WEEK_COMMENCING` |
| `MAX VALUE PEAK INT VAL (€)` | `MAX_VALUE_PEAK_INT_VAL__EUR_` |
| `VPL - ADJ` | `VPL___ADJ` |
| `VPC  - ADJ` | `VPC____ADJ` |
| `VPS  - ADJ` | `VPS____ADJ` |
| `VPV  - ADJ` | `VPV____ADJ` |
| `ADVALUE_LIKES - ADJ` | `ADVALUE_LIKES___ADJ` |
| `ADVALUE_COMMENTS - ADJ` | `ADVALUE_COMMENTS___ADJ` |
| `ADVALUE_SHARES - ADJ` | `ADVALUE_SHARES___ADJ` |
| `ADVALUE_VIEWS - ADJ` | `ADVALUE_VIEWS___ADJ` |
| `MAX VALUE PEAK INT VAL (€) - ADJ` | `MAX_VALUE_PEAK_INT_VAL__EUR____ADJ` |
| `FIXED PROMO TYPE` | `FIXED_PROMO_TYPE` |
| `FIXED PROMO SCORE` | `FIXED_PROMO_SCORE` |
| `Media Value (Level 1)_Current` | `Media_Value__Level_1__Current` |
| `Media Value (Level 6)_Current` | `Media_Value__Level_6__Current` |
| `Media Value (Level 2)_New` | `Media_Value__Level_2__New` |
| `Media Value (Level 3)_New` | `Media_Value__Level_3__New` |
| `Media Value (Level 4)_New` | `Media_Value__Level_4__New` |
| `Media Value (Level 5)_New` | `Media_Value__Level_5__New` |
| `CPE LOOKUP PLATFORM POST TYPE` | `CPE_LOOKUP_PLATFORM_POST_TYPE` |
| `Album PQ Applied` | `Album_PQ_Applied` |
| `Orignal views` | `Orignal_views` |

*2526_ueluecl_valuation_uniquememberposts*
| Bronze | Silver |
|---|---|
| `COVID TIME PERIOD` | `COVID_TIME_PERIOD` |

---

## II) Data Transformation and Quality Assessment (SILVER, SILVER → GOLD)

**Notebook:** `TEAM > Social data_2_Data quality check and transfer to Gold`

Runs all quality checks against silver tables. Rows passing all checks are combined into 4 master gold tables. Rows failing at least one check are written to 4 quarantine tables with a `_failure_reasons` column explaining every failed check per row. A `competition` column is added to every row before combining so no information is lost when unioning UCL, UEL, UECL and UELUECL tables.

---

#### 0. Config

Sets the catalog and schema names, the dataset type map and competition map used throughout the notebook.

```python
CATALOG = "team"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
DIM_SCHEMA = "dim"

DATASET_TYPE_MAP = {
    "audience_byposttype":         "audience_byposttype",
    "audience_byday":              "audience_byday",
    "valuation_memberposts":       "valuation_memberposts",
    "valuation_uniquememberposts": "valuation_uniquememberposts",
}

# Order matters: ueluecl must come before uel and ucl to avoid partial match
COMPETITION_MAP = {
    "ueluecl": "UELUECL",
    "ucl":     "UCL",
    "uel":     "UEL",
    "uecl":    "UECL",
}
```

---

#### 1. Load DIM Tables into Memory

Reads all DIM tables from `team.dim` and loads the allowed values into Python sets for fast lookup. This happens once at the start so checks run fast without repeatedly hitting the database.

```python
def load_dim(table_name, value_column):
    rows = spark.table(f"{CATALOG}.{DIM_SCHEMA}.{table_name}").select(value_column).collect()
    return set(str(r[value_column]) for r in rows)

dim_platform    = load_dim("social_platform", "platform_value")
dim_stage       = load_dim("umcc_stage", "stage_value")
dim_month       = load_dim("month", "month_value")
dim_gs_ko       = load_dim("umcc_gs_ko", "gs_ko_value")
dim_covid_dates = load_dim("umcc_covid_dates", "covid_dates_value")
dim_grouptype   = load_dim("grouptype", "grouptype_value")
dim_logopresent = load_dim("logopresent", "logopresent_value")
dim_textpresent = load_dim("textpresent", "textpresent_value")
dim_weekday     = load_dim("weekday", "weekday_value")
```

---

#### 2. Check Functions

Defines all reusable check types. Each function takes a DataFrame and a column name and returns a boolean Spark column — `True` = check passed, `False` = check failed. All functions are defined once and reused across every table.

| Check Type | Description |
|---|---|
| `not_null` | Fails if the value is null |
| `non_negative` | Fails if null or < 0 |
| `positive` | Fails if null or <= 0 |
| `null_or_non_negative` | Passes if blank or >= 0 |
| `null_or_value` | Passes if blank or equals a specific string |
| `string_pattern` | Fails if null or doesn't match a regex pattern |
| `dim_lookup` | Fails if value not in the DIM allowed values list |
| `null_or_dim_lookup` | Passes if blank or value is in the DIM allowed values list |
| `numeric_range` | Fails if outside min/max range |
| `date_not_null` | Fails if date column is null |
| `sum_equals` | Fails if column doesn't equal sum of other columns |

```python
from pyspark.sql import functions as F

def check_not_null(df, column, **params):
    return F.col(column).isNotNull()

def check_non_negative(df, column, **params):
    return F.expr(f"try_cast({column} AS BIGINT)").isNotNull() & \
           (F.expr(f"try_cast({column} AS BIGINT)") >= 0)

def check_positive(df, column, **params):
    return F.expr(f"try_cast({column} AS BIGINT)").isNotNull() & \
           (F.expr(f"try_cast({column} AS BIGINT)") > 0)

def check_null_or_non_negative(df, column, **params):
    return F.col(column).isNull() | \
           (F.expr(f"try_cast({column} AS BIGINT)") >= 0)

def check_null_or_value(df, column, allowed_value, **params):
    return F.col(column).isNull() | (F.col(column) == allowed_value)

def check_string_pattern(df, column, pattern, **params):
    return F.col(column).isNotNull() & F.col(column).rlike(pattern)

def check_dim_lookup(df, column, allowed_values, **params):
    allowed_lower = [v.lower() for v in allowed_values]
    return F.col(column).isNotNull() & F.col(column).isin(allowed_lower)

def check_null_or_dim_lookup(df, column, allowed_values, **params):
    allowed_lower = [v.lower() for v in allowed_values]
    return F.col(column).isNull() | F.col(column).isin(allowed_lower)

def check_numeric_range(df, column, min_value=None, max_value=None, **params):
    casted = F.expr(f"try_cast({column} AS DOUBLE)")
    cond = casted.isNotNull()
    if min_value is not None:
        cond = cond & (casted >= min_value)
    if max_value is not None:
        cond = cond & (casted <= max_value)
    return cond

def check_date_not_null(df, column, **params):
    return F.col(column).isNotNull()

def check_sum_equals(df, column, addends, **params):
    cast_addends = [f"try_cast({a} AS BIGINT)" for a in addends]
    sum_expr = " + ".join(cast_addends)
    return F.col(column).isNotNull() & \
           (F.expr(f"try_cast({column} AS BIGINT)") == F.expr(sum_expr))
```

---

#### 3. Column-Level Rules Registry

The rules registry is the only section that needs to be edited when adding or changing quality checks. Each column has exactly one rule — which check type to use, any parameters, and the human-readable error message shown in the quarantine table. Columns not listed are not checked.

```python
COLUMN_RULES = {

    # --- Mandatory text fields ---
    "POSTEDBY":         {"check_type": "not_null", "params": {}, "message": "POSTEDBY is missing — field is mandatory"},
    "OVERALL_PARTNER":  {"check_type": "not_null", "params": {}, "message": "OVERALL_PARTNER is missing — field is mandatory"},
    "PARTNER":          {"check_type": "not_null", "params": {}, "message": "PARTNER is missing — field is mandatory"},
    "FIXED_PROMO_TYPE": {"check_type": "not_null", "params": {}, "message": "FIXED_PROMO_TYPE is missing — field is mandatory"},

    # --- URL ---
    "POSTURL": {"check_type": "string_pattern", "params": {"pattern": r"^http"}, "message": "POSTURL must start with 'http'"},

    # --- Date fields ---
    "DATEPOSTED":      {"check_type": "date_not_null", "params": {}, "message": "DATEPOSTED is missing or could not be parsed as a valid date (expected DD/MM/YYYY)"},
    "DATE":            {"check_type": "date_not_null", "params": {}, "message": "DATE is missing or could not be parsed as a valid date (expected DD/MM/YYYY)"},
    "WEEK_COMMENCING": {"check_type": "date_not_null", "params": {}, "message": "WEEK_COMMENCING is missing or could not be parsed as a valid date (expected DD/MM/YYYY)"},
    "EU_DATE":         {"check_type": "date_not_null", "params": {}, "message": "EU_DATE is missing or could not be parsed as a valid date (expected DD/MM/YYYY)"},

    # --- DIM lookups ---
    "PLATFORM":    {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "PLATFORM value is not in the allowed list (Instagram, Tiktok, Twitter, Youtube, Weibo, VK, Facebook)", "dim_ref": "dim_platform"},
    "STAGE":       {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "STAGE must be exactly GS, KO or LP",                                                                   "dim_ref": "dim_stage"},
    "MONTH":       {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "MONTH must be a valid calendar month name",                                                             "dim_ref": "dim_month"},
    "GS_KO":       {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "GS_KO must be exactly GS or KO",                                                                       "dim_ref": "dim_gs_ko"},
    "COVID_DATES": {"check_type": "null_or_dim_lookup", "params": {"allowed_values": None}, "message": "COVID_DATES must be blank or exactly: Pre COVID, COVID, Post COVID or NOT MATCHWEEK",                  "dim_ref": "dim_covid_dates"},
    "GROUPTYPE":   {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "GROUPTYPE must be exactly Owned or UnOwned",                                                            "dim_ref": "dim_grouptype"},
    "LOGOPRESENT": {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "LOGOPRESENT must be exactly 0 or 1",                                                                   "dim_ref": "dim_logopresent"},
    "TEXTPRESENT": {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "TEXTPRESENT must be exactly 0 or 1",                                                                   "dim_ref": "dim_textpresent"},
    "WEEKDAY":     {"check_type": "dim_lookup",         "params": {"allowed_values": None}, "message": "WEEKDAY must be a valid day name (Monday through Sunday)",                                              "dim_ref": "dim_weekday"},

    # --- Must be >= 0 ---
    "LIKES":                             {"check_type": "non_negative", "params": {}, "message": "LIKES must be >= 0"},
    "COMMENTS":                          {"check_type": "non_negative", "params": {}, "message": "COMMENTS must be >= 0"},
    "SHARES":                            {"check_type": "non_negative", "params": {}, "message": "SHARES must be >= 0"},
    "VIDEOVIEWS":                        {"check_type": "non_negative", "params": {}, "message": "VIDEOVIEWS must be >= 0"},
    "INTERACTIONS_VIEWS":                {"check_type": "non_negative", "params": {}, "message": "INTERACTIONS_VIEWS must be >= 0"},
    "IMPRESSIONS":                       {"check_type": "non_negative", "params": {}, "message": "IMPRESSIONS must be >= 0"},
    "VIEWS":                             {"check_type": "non_negative", "params": {}, "message": "VIEWS must be >= 0"},
    "VIDEO_POSTS":                       {"check_type": "non_negative", "params": {}, "message": "VIDEO_POSTS must be >= 0"},
    "VIDEO_POST_VIEWS":                  {"check_type": "non_negative", "params": {}, "message": "VIDEO_POST_VIEWS must be >= 0"},
    "IMAGE_POSTS":                       {"check_type": "non_negative", "params": {}, "message": "IMAGE_POSTS must be >= 0"},
    "IMAGE_INTERACTIONS":                {"check_type": "non_negative", "params": {}, "message": "IMAGE_INTERACTIONS must be >= 0"},
    "INSTAGRAM_STORYIMAGE_POSTS":        {"check_type": "non_negative", "params": {}, "message": "INSTAGRAM_STORYIMAGE_POSTS must be >= 0"},
    "INSTAGRAM_STORYIMAGE_INTERACTIONS": {"check_type": "non_negative", "params": {}, "message": "INSTAGRAM_STORYIMAGE_INTERACTIONS must be >= 0"},
    "INSTAGRAM_STORYVIDEO_POSTS":        {"check_type": "non_negative", "params": {}, "message": "INSTAGRAM_STORYVIDEO_POSTS must be >= 0"},
    "INSTAGRAM_STORYVIDEO_INTERACTIONS": {"check_type": "non_negative", "params": {}, "message": "INSTAGRAM_STORYVIDEO_INTERACTIONS must be >= 0"},
    "MAXADVVALUE":                       {"check_type": "non_negative", "params": {}, "message": "MAXADVVALUE must be >= 0"},
    "V3_PROMOTIONQUALITY":               {"check_type": "non_negative", "params": {}, "message": "V3_PROMOTIONQUALITY must be >= 0"},
    "V3_TOTALVALUE":                     {"check_type": "non_negative", "params": {}, "message": "V3_TOTALVALUE must be >= 0"},
    "VPL":                               {"check_type": "non_negative", "params": {}, "message": "VPL must be >= 0"},
    "VPC":                               {"check_type": "non_negative", "params": {}, "message": "VPC must be >= 0"},
    "VPS":                               {"check_type": "non_negative", "params": {}, "message": "VPS must be >= 0"},
    "VPV":                               {"check_type": "non_negative", "params": {}, "message": "VPV must be >= 0"},
    "ADVVALUE_LIKES":                    {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_LIKES must be >= 0"},
    "ADVVALUE_COMMENTS":                 {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_COMMENTS must be >= 0"},
    "ADVVALUE_SHARES":                   {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_SHARES must be >= 0"},
    "ADVVALUE_VIEWS":                    {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_VIEWS must be >= 0"},
    "MAX_VALUE_PEAK_INT_VAL__EUR__":     {"check_type": "non_negative", "params": {}, "message": "MAX_VALUE_PEAK_INT_VAL (EUR) must be >= 0"},
    "VPL_EUR_ADJ":                       {"check_type": "non_negative", "params": {}, "message": "VPL_EUR_ADJ must be >= 0"},
    "VPC__EUR_ADJ":                      {"check_type": "non_negative", "params": {}, "message": "VPC_EUR_ADJ must be >= 0"},
    "VPS__EUR_ADJ":                      {"check_type": "non_negative", "params": {}, "message": "VPS_EUR_ADJ must be >= 0"},
    "VPV__EUR_ADJ":                      {"check_type": "non_negative", "params": {}, "message": "VPV_EUR_ADJ must be >= 0"},
    "ADVVALUE_LIKES_EUR_ADJ":            {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_LIKES_EUR_ADJ must be >= 0"},
    "ADVVALUE_COMMENTS_EUR_ADJ":         {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_COMMENTS_EUR_ADJ must be >= 0"},
    "ADVVALUE_SHARES_EUR_ADJ":           {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_SHARES_EUR_ADJ must be >= 0"},
    "ADVVALUE_VIEWS_EUR_ADJ":            {"check_type": "non_negative", "params": {}, "message": "ADVVALUE_VIEWS_EUR_ADJ must be >= 0"},
    "MAX_VALUE_PEAK_INT_VAL__EUR__EUR_ADJ": {"check_type": "non_negative", "params": {}, "message": "MAX_VALUE_PEAK_INT_VAL (EUR) ADJ must be >= 0"},
    "Media_Value__Level_1__Current":     {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 1) Current must be >= 0"},
    "Media_Value__Level_6__Current":     {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 6) Current must be >= 0"},
    "Media_Value__Level_2__New":         {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 2) New must be >= 0"},
    "Media_Value__Level_3__New":         {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 3) New must be >= 0"},
    "Media_Value__Level_4__New":         {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 4) New must be >= 0"},
    "Media_Value__Level_5__New":         {"check_type": "non_negative", "params": {}, "message": "Media_Value (Level 5) New must be >= 0"},

    # --- Must be > 0 ---
    "POSTS": {"check_type": "positive", "params": {}, "message": "POSTS must be > 0"},

    # --- Blank or >= 0 ---
    "OWNED_IMPRESSIONS":              {"check_type": "null_or_non_negative", "params": {}, "message": "OWNED_IMPRESSIONS must be blank or >= 0"},
    "IG_Total_Album_Views":           {"check_type": "null_or_non_negative", "params": {}, "message": "IG_Total_Album_Views must be blank or >= 0"},
    "IG_ALBUM_FIRST_TILE_VIEWS":      {"check_type": "null_or_non_negative", "params": {}, "message": "IG_ALBUM_FIRST_TILE_VIEWS must be blank or >= 0"},
    "UEFA_IG_ALBUM_FIRST_TILE_VIEWS": {"check_type": "null_or_non_negative", "params": {}, "message": "UEFA_IG_ALBUM_FIRST_TILE_VIEWS must be blank or >= 0"},
    "UEFA_TOTAL_IG_ALBUM_VIEWS":      {"check_type": "null_or_non_negative", "params": {}, "message": "UEFA_TOTAL_IG_ALBUM_VIEWS must be blank or >= 0"},
    "Album_PQ_Applied":               {"check_type": "null_or_non_negative", "params": {}, "message": "Album_PQ_Applied must be blank or >= 0"},

    # --- Blank or "Yes" ---
    "COVID_2019": {"check_type": "null_or_value", "params": {"allowed_value": "yes"}, "message": "COVID_2019 must be blank or 'Yes'"},

    # --- Numeric range ---
    "FIXED_PROMO_SCORE": {"check_type": "numeric_range", "params": {"min_value": 0, "max_value": 1}, "message": "FIXED_PROMO_SCORE must be between 0 and 1"},

    # --- Cross-column sum checks ---
    "TOTAL_INTERACTIONS":       {"check_type": "sum_equals", "params": {"addends": ["LIKES", "COMMENTS", "SHARES"]},          "message": "TOTAL_INTERACTIONS must equal LIKES + COMMENTS + SHARES"},
    "TOTAL_INTERACTIONS_VIEWS": {"check_type": "sum_equals", "params": {"addends": ["LIKES", "COMMENTS", "SHARES", "VIEWS"]}, "message": "TOTAL_INTERACTIONS_VIEWS must equal LIKES + COMMENTS + SHARES + VIEWS"},
    "MSI":                      {"check_type": "sum_equals", "params": {"addends": ["LIKES", "COMMENTS", "SHARES"]},          "message": "MSI must equal LIKES + COMMENTS + SHARES"},
}
```

---

#### 4. Resolve DIM References at Runtime

The DIM lookup rules in Section 3 reference a DIM set by name (e.g. `"dim_platform"`). This section replaces those names with the actual sets loaded in Section 1. Kept separate so Section 3 stays clean and readable.

```python
DIM_REFS = {
    "dim_platform":    dim_platform,
    "dim_stage":       dim_stage,
    "dim_month":       dim_month,
    "dim_gs_ko":       dim_gs_ko,
    "dim_covid_dates": dim_covid_dates,
    "dim_grouptype":   dim_grouptype,
    "dim_logopresent": dim_logopresent,
    "dim_textpresent": dim_textpresent,
    "dim_weekday":     dim_weekday,
}

for col_name, rule in COLUMN_RULES.items():
    if "dim_ref" in rule:
        rule["params"]["allowed_values"] = DIM_REFS[rule["dim_ref"]]
```

---

#### 5. Table Discovery from Silver

Lists all tables in the silver schema automatically, excluding any quarantine tables. Defines two helper functions to extract competition and dataset type from the table name.

```python
def list_silver_tables(catalog, schema):
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    return [r["tableName"] for r in rows if not r["tableName"].endswith("_quarantine")]

def extract_competition(table_name):
    for key, label in COMPETITION_MAP.items():
        if key in table_name:
            return label
    return "UNKNOWN"

def extract_dataset_type(table_name):
    for suffix in DATASET_TYPE_MAP.keys():
        if suffix in table_name:
            return suffix
    return None
```

---

#### 6. Run Checks and Collect Results per Competition

For every silver table: adds the `competition` column (only if not already present — UELUECL valuation tables already have it per row), finds which rules apply based on exact column name match, runs all applicable checks, collects all failure reasons per row into a `_failure_reasons` array, and splits rows into clean and quarantine DataFrames held in memory buckets grouped by dataset type.

```python
from functools import reduce

clean_buckets = {k: [] for k in DATASET_TYPE_MAP.keys()}
quarantine_buckets = {k: [] for k in DATASET_TYPE_MAP.keys()}

for table_name in silver_tables:
    silver_path = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    competition = extract_competition(table_name)
    dataset_type = extract_dataset_type(table_name)

    if dataset_type is None:
        print(f"[{table_name}] WARNING: no dataset type match — skipping")
        continue

    df = spark.table(silver_path)

    if "competition" not in df.columns:
        df = df.withColumn("competition", F.lit(competition))

    cols = ["competition"] + [c for c in df.columns if c != "competition"]
    df = df.select(cols)

    table_columns = set(df.columns)
    applicable_rules = {
        col: rule for col, rule in COLUMN_RULES.items()
        if col in table_columns
    }

    failure_exprs = []
    for col_name, rule in applicable_rules.items():
        check_fn = CHECK_FUNCTIONS[rule["check_type"]]
        passed_col = check_fn(df, col_name, **rule["params"])
        failure_exprs.append(
            F.when(~passed_col, F.lit(rule["message"])).otherwise(F.lit(None))
        )

    df_checked = df.withColumn(
        "_failure_reasons",
        F.array_except(
            F.array(*failure_exprs),
            F.array(F.lit(None).cast("string"))
        )
    )

    clean_df = df_checked.filter(F.size(F.col("_failure_reasons")) == 0).drop("_failure_reasons")
    quarantine_df = df_checked.filter(F.size(F.col("_failure_reasons")) > 0)

    clean_buckets[dataset_type].append(clean_df)
    quarantine_buckets[dataset_type].append(quarantine_df)
```

---

#### 7. Union into Master Gold + Quarantine Tables

Unions all competition DataFrames per dataset type and writes to gold. `competition` is the first column in every combined table. Uses `unionByName(allowMissingColumns=True)` to handle cases where columns differ slightly between competition tables.

```python
for dataset_type, gold_table_name in DATASET_TYPE_MAP.items():
    gold_path = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}"
    quarantine_path = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}_quarantine"

    clean_dfs = clean_buckets[dataset_type]
    if clean_dfs:
        combined_clean = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), clean_dfs)
        combined_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_path)

    quarantine_dfs = quarantine_buckets[dataset_type]
    if quarantine_dfs:
        combined_quarantine = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), quarantine_dfs)
        combined_quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_path)
```

---

## III) Data Enrichment and Business Analysis Readiness (GOLD)

The gold schema contains 8 tables split into 4 clean datasets and 4 quarantine datasets. The clean tables — `audience_byday`, `audience_byposttype`, `valuation_memberposts` and `valuation_uniquememberposts` — contain all rows that passed every quality check and are ready for business analysis and Power BI reporting. Each table combines data across all competitions (UCL, UEL, UECL, UELUECL) with a `competition` column to allow filtering by competition in reporting. The quarantine tables — `audience_byday_quarantine`, `audience_byposttype_quarantine`, `valuation_memberposts_quarantine` and `valuation_uniquememberposts_quarantine` — contain all rows that failed at least one quality check, with a `_failure_reasons` column listing every check that failed per row. Quarantine tables are reviewed after each pipeline run to identify and correct data quality issues in the source Excel files before re-uploading and re-running the pipeline.

**Clean tables (ready for analysis):**

| Table | Description |
|---|---|
| `team.gold.audience_byday` | Audience metrics broken down by day, combined across all competitions |
| `team.gold.audience_byposttype` | Audience metrics broken down by post type, combined across all competitions |
| `team.gold.valuation_memberposts` | Media valuation data for member posts, combined across all competitions |
| `team.gold.valuation_uniquememberposts` | Media valuation data for unique member posts, combined across all competitions |

**Quarantine tables (failed quality checks):**

| Table | Description |
|---|---|
| `team.gold.audience_byday_quarantine` | Rows from audience_byday that failed at least one quality check |
| `team.gold.audience_byposttype_quarantine` | Rows from audience_byposttype that failed at least one quality check |
| `team.gold.valuation_memberposts_quarantine` | Rows from valuation_memberposts that failed at least one quality check |
| `team.gold.valuation_uniquememberposts_quarantine` | Rows from valuation_uniquememberposts that failed at least one quality check |

---

## IV) Pipeline Management

**Notebook:** `TEAM > Social data_4_Pipeline Run Summary`

Provides a full row count overview across all pipeline layers — bronze, silver and gold — so you can verify at a glance how many rows were transferred at each stage and how many ended up in quarantine. Can be run at any time independently of the pipeline job.

---

#### 0. Config

```python
CATALOG = "team"
BRONZE_SCHEMA = "landing_bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

DATASET_TYPE_MAP = {
    "audience_byposttype":         "audience_byposttype",
    "audience_byday":              "audience_byday",
    "valuation_memberposts":       "valuation_memberposts",
    "valuation_uniquememberposts": "valuation_uniquememberposts",
}

COMPETITION_MAP = {
    "ueluecl": "UELUECL",
    "ucl":     "UCL",
    "uel":     "UEL",
    "uecl":    "UECL",
}
```

---

#### 1. Bronze & Silver — Row Counts per Source Table

Lists all bronze tables, matches each to its competition and dataset type, and counts rows in both bronze and silver. Displays one row per source table so you can verify no rows were lost during the bronze to silver transfer.

```python
def extract_competition(table_name):
    for key, label in COMPETITION_MAP.items():
        if key in table_name:
            return label
    return "UNKNOWN"

def extract_dataset_type(table_name):
    for suffix in DATASET_TYPE_MAP.keys():
        if suffix in table_name:
            return suffix
    return None

def safe_count(path):
    try:
        return spark.table(path).count()
    except:
        return None

bronze_tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
]

bronze_silver_summary = []

for table_name in bronze_tables:
    dataset_type = extract_dataset_type(table_name)
    competition = extract_competition(table_name)

    if dataset_type is None:
        continue

    bronze_count = safe_count(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    silver_count = safe_count(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")

    bronze_silver_summary.append({
        "source_table":  table_name,
        "competition":   competition,
        "dataset_type":  dataset_type,
        "bronze_rows":   bronze_count,
        "silver_rows":   silver_count,
    })

display(
    spark.createDataFrame(bronze_silver_summary)
    .orderBy("dataset_type", "competition")
)
```

**Output columns:**

| Column | Description |
|---|---|
| `source_table` | Bronze table name |
| `competition` | Competition extracted from table name |
| `dataset_type` | Dataset type extracted from table name |
| `bronze_rows` | Row count in bronze |
| `silver_rows` | Row count in silver |

---

#### 2. Gold — Row Counts per Dataset Type

Shows clean vs quarantine row counts for each of the 4 combined gold tables, including percentage split. Combined across all competitions since gold tables are unified.

```python
gold_summary = []

for dataset_type, gold_table_name in DATASET_TYPE_MAP.items():
    gold_path = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}"
    quarantine_path = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}_quarantine"

    gold_clean_count = safe_count(gold_path)
    gold_quarantine_count = safe_count(quarantine_path)

    total = (gold_clean_count or 0) + (gold_quarantine_count or 0)
    clean_pct = round(gold_clean_count / total * 100, 1) if total > 0 else None
    quarantine_pct = round(gold_quarantine_count / total * 100, 1) if total > 0 else None

    gold_summary.append({
        "dataset_type":         dataset_type,
        "gold_clean_rows":      gold_clean_count,
        "gold_clean_pct":       clean_pct,
        "gold_quarantine_rows": gold_quarantine_count,
        "gold_quarantine_pct":  quarantine_pct,
        "total_rows":           total,
    })

display(
    spark.createDataFrame(gold_summary)
    .orderBy("dataset_type")
)
```

**Output columns:**

| Column | Description |
|---|---|
| `dataset_type` | Dataset type |
| `gold_clean_rows` | Rows that passed all quality checks |
| `gold_clean_pct` | Percentage of total rows that are clean |
| `gold_quarantine_rows` | Rows that failed at least one quality check |
| `gold_quarantine_pct` | Percentage of total rows in quarantine |
| `total_rows` | Total rows across clean and quarantine |

---

#### 3. Quarantine Breakdown — Row Count per Error Message by Dataset Type and Competition

Explodes the `_failure_reasons` array in the quarantine tables into individual rows and counts how many times each error message appears per dataset type and competition. Also shows the total silver row count per competition for context.

```python
from functools import reduce
from pyspark.sql import functions as F

silver_lookup = {
    (r["dataset_type"], r["competition"]): r["silver_rows"]
    for r in bronze_silver_summary
}

error_summary = []

for dataset_type, gold_table_name in DATASET_TYPE_MAP.items():
    quarantine_path = f"{CATALOG}.{GOLD_SCHEMA}.{gold_table_name}_quarantine"

    try:
        df = spark.table(quarantine_path)

        if df.count() == 0:
            continue

        df_exploded = df.select(
            F.lit(dataset_type).alias("dataset_type"),
            F.col("competition"),
            F.explode(F.col("_failure_reasons")).alias("error_message")
        ).withColumn(
            "column_name",
            F.split(F.col("error_message"), " ").getItem(0)
        )

        df_counts = df_exploded.groupBy("dataset_type", "competition", "column_name", "error_message") \
                               .count() \
                               .withColumnRenamed("count", "row_count") \
                               .orderBy("dataset_type", "competition", F.col("row_count").desc())

        df_counts = df_counts.withColumn(
            "total_silver_rows",
            F.udf(lambda dt, comp: silver_lookup.get((dt, comp)))(F.col("dataset_type"), F.col("competition"))
        )

        error_summary.append(df_counts)

    except Exception as e:
        print(f"ERROR reading {quarantine_path}: {str(e)[:100]}")

if error_summary:
    combined = reduce(lambda a, b: a.union(b), error_summary)
    display(combined.toPandas())
else:
    print("No quarantine rows found across any dataset type.")
```

**Output columns:**

| Column | Description |
|---|---|
| `dataset_type` | Dataset type |
| `competition` | Competition |
| `column_name` | Column that failed the check |
| `error_message` | Full human-readable error message |
| `row_count` | Number of rows that failed this specific check |
| `total_silver_rows` | Total rows in silver for this dataset type and competition |
