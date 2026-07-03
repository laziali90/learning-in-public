# Check Notebooks

Supporting diagnostic notebooks to verify data quality and column types before and after pipeline runs. These are not part of the main pipeline job but should be run when investigating data issues.

---

## Social data_CHECK_Column Type

**Notebook:** `TEAM > Social data_CHECK_column type`

Validates that all columns in the bronze tables have the expected data types before the pipeline runs. If any column type mismatches are found, the notebook raises an error to stop the job immediately, preventing bad data from flowing into silver and gold.

**Run this before the pipeline job whenever new files are uploaded to bronze.**

---

### 0. Config

```python
CATALOG = "team"
BRONZE_SCHEMA = "landing_bronze"
```

---

### 1. Expected Column Types Registry

Keyed by column name. Wherever a column with this exact name appears in any bronze table, the expected type is checked against the actual type. Columns not listed here are not checked.

Note: column names here reflect the silver naming convention (after special character cleaning) since Databricks may have already applied type inference on ingestion using the cleaned names.

```python
EXPECTED_TYPES = {

    # --- STRING columns ---
    "SOCIALPOST_ID":    "string",
    "POSTEDBY":         "string",
    "PLATFORM":         "string",
    "POSTURL":          "string",
    "POST_TIME":        "string",
    "WEEKDAY":          "string",
    "SEASON":           "string",
    "STAGE":            "string",
    "MONTH":            "string",
    "GS_KO":            "string",
    "COVID_DATES":      "string",
    "COVID_2019":       "string",
    "GROUPTYPE":        "string",
    "FIXED_PROMO_TYPE": "string",
    "OVERALL_PARTNER":  "string",
    "PARTNER":          "string",

    # --- DATE columns ---
    "DATEPOSTED":      "date",
    "DATE":            "date",
    "WEEK_COMMENCING": "date",
    "EU_DATE":         "date",

    # --- BIGINT columns (whole numbers) ---
    "LIKES":                             "bigint",
    "COMMENTS":                          "bigint",
    "SHARES":                            "bigint",
    "VIDEOVIEWS":                        "bigint",
    "INTERACTIONS_VIEWS":                "bigint",
    "IMPRESSIONS":                       "bigint",
    "VIEWS":                             "bigint",
    "POSTS":                             "bigint",
    "TOTAL_INTERACTIONS":                "bigint",
    "TOTAL_INTERACTIONS_VIEWS":          "bigint",
    "VIDEO_POSTS":                       "bigint",
    "VIDEO_POST_VIEWS":                  "bigint",
    "IMAGE_POSTS":                       "bigint",
    "IMAGE_INTERACTIONS":                "bigint",
    "INSTAGRAM_STORYIMAGE_POSTS":        "bigint",
    "INSTAGRAM_STORYIMAGE_INTERACTIONS": "bigint",
    "INSTAGRAM_STORYVIDEO_POSTS":        "bigint",
    "INSTAGRAM_STORYVIDEO_INTERACTIONS": "bigint",
    "OWNED_IMPRESSIONS":                 "bigint",
    "IG_Total_Album_Views":              "bigint",
    "IG_ALBUM_FIRST_TILE_VIEWS":         "bigint",
    "UEFA_IG_ALBUM_FIRST_TILE_VIEWS":    "bigint",
    "UEFA_TOTAL_IG_ALBUM_VIEWS":         "bigint",
    "MSI":                               "bigint",
    "_VIDEOVIEWS":                       "bigint",
    "LOGOPRESENT":                       "bigint",
    "TEXTPRESENT":                       "bigint",

    # --- DOUBLE columns (decimal numbers) ---
    "MAXADVVALUE":                       "double",
    "V3_PROMOTIONQUALITY":               "double",
    "V3_TOTALVALUE":                     "double",
    "VPL":                               "double",
    "VPC":                               "double",
    "VPS":                               "double",
    "VPV":                               "double",
    "ADVVALUE_LIKES":                    "double",
    "ADVVALUE_COMMENTS":                 "double",
    "ADVVALUE_SHARES":                   "double",
    "ADVVALUE_VIEWS":                    "double",
    "MAX_VALUE_PEAK_INT_VAL__EUR__":     "double",
    "VPL_EUR_ADJ":                       "double",
    "VPC__EUR_ADJ":                      "double",
    "VPS__EUR_ADJ":                      "double",
    "VPV__EUR_ADJ":                      "double",
    "ADVVALUE_LIKES_EUR_ADJ":            "double",
    "ADVVALUE_COMMENTS_EUR_ADJ":         "double",
    "ADVVALUE_SHARES_EUR_ADJ":           "double",
    "ADVVALUE_VIEWS_EUR_ADJ":            "double",
    "MAX_VALUE_PEAK_INT_VAL__EUR__EUR_ADJ": "double",
    "FIXED_PROMO_SCORE":                 "double",
    "Media_Value__Level_1__Current":     "double",
    "Media_Value__Level_6__Current":     "double",
    "Media_Value__Level_2__New":         "double",
    "Media_Value__Level_3__New":         "double",
    "Media_Value__Level_4__New":         "double",
    "Media_Value__Level_5__New":         "double",
    "Media_Value__Level_4__New_-_Hookit_Original_Value_for_Carousel": "double",
    "Album_PQ_Applied":                  "double",
}
```

---

### 2. Run Type Checks Against All Bronze Tables

Applies the same column name cleaning logic as the bronze to silver transfer (replacing `€` with `EUR` and all remaining special characters with underscores) before comparing actual types against expected types.

```python
import re

def clean_col_name(col):
    """Apply same cleaning logic as bronze to silver transfer."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', col.replace('€', 'EUR'))

bronze_tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
]

mismatches = []

for table_name in bronze_tables:
    bronze_path = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    df = spark.table(bronze_path)

    actual_types = {
        clean_col_name(col): dtype
        for col, dtype in df.dtypes
    }

    for col_name, expected_type in EXPECTED_TYPES.items():
        if col_name in actual_types:
            actual_type = actual_types[col_name]
            if actual_type != expected_type:
                mismatches.append({
                    "dataset":       table_name,
                    "column":        col_name,
                    "expected_type": expected_type,
                    "actual_type":   actual_type,
                })
```

---

### 3. Results

If mismatches are found, displays a table showing the dataset, column, expected type and actual type, then raises an exception to stop the job. If all types are correct, prints a confirmation and the pipeline proceeds.

```python
if mismatches:
    display(spark.createDataFrame(mismatches).orderBy("dataset", "column"))
    raise Exception(
        f"{len(mismatches)} column type mismatch(es) found in bronze tables. "
        f"Please fix the source Excel files and re-upload before running the pipeline."
    )
else:
    print("All column types match expected types. Pipeline can proceed.")
```

**Output columns when mismatches are found:**

| Column | Description |
|---|---|
| `dataset` | Bronze table name where the mismatch was found |
| `column` | Column name with the wrong type |
| `expected_type` | The type the pipeline expects |
| `actual_type` | The type actually found in bronze |

---

## Social data_CHECK_Landing Bronze XLS Formula Errors

**Notebook:** `TEAM > Social data_CHECK_Landing bronze xls formula errors`

Scans all tables in the bronze schema for Excel error values (e.g. `#N/A`, `#DIV/0!`, `#VALUE!` etc.) and reports the count of errors by dataset, column and error type. All error types are checked in a single aggregation per column rather than separate scans per error type for performance.

---

### 0. Config

```python
CATALOG = "team"
BRONZE_SCHEMA = "landing_bronze"
```

---

### 1. Scan All Bronze Tables for Excel Errors

Loops through every bronze table, checks every string column for Excel error patterns in a single aggregation pass, and records any errors found.

```python
from pyspark.sql import functions as F

EXCEL_ERRORS = {
    "N/A":    r"^(ERROR:)?#N/A$",
    "DIV/0!": r"^(ERROR:)?#DIV/0!$",
    "VALUE!": r"^(ERROR:)?#VALUE!$",
    "REF!":   r"^(ERROR:)?#REF!$",
    "NAME?":  r"^(ERROR:)?#NAME\?$",
    "NULL!":  r"^(ERROR:)?#NULL!$",
    "NUM!":   r"^(ERROR:)?#NUM!$",
}

bronze_tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
]

results = []

for table_name in bronze_tables:
    bronze_path = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    df = spark.table(bronze_path)
    total_rows = df.count()

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df_with_flags = df.select(
                *[
                    F.when(F.col(col_name).rlike(pattern), 1)
                     .otherwise(0)
                     .alias(error_type)
                    for error_type, pattern in EXCEL_ERRORS.items()
                ]
            )

            agg_exprs = [F.sum(error_type).alias(error_type) for error_type in EXCEL_ERRORS]
            agg_result = df_with_flags.agg(*agg_exprs).collect()[0]

            for error_type in EXCEL_ERRORS:
                error_count = agg_result[error_type]
                if error_count > 0:
                    results.append({
                        "dataset":           table_name,
                        "column":            col_name,
                        "column_type":       dtype,
                        "error_type":        error_type,
                        "excel_error_count": error_count,
                        "total_rows":        total_rows,
                        "error_pct":         round(error_count / total_rows * 100, 2),
                    })
```

---

### 2. Results

```python
if results:
    display(
        spark.createDataFrame(results)
        .orderBy("dataset", "column", "error_type")
    )
    print(f"\nTotal columns with Excel errors: {len(results)}")
else:
    print("No Excel errors found in any bronze table.")
```

**Output columns:**

| Column | Description |
|---|---|
| `dataset` | Bronze table name |
| `column` | Column containing the error |
| `column_type` | Data type of the column |
| `error_type` | Excel error type (e.g. `N/A`, `DIV/0!`) |
| `excel_error_count` | Number of cells with this error in this column |
| `total_rows` | Total rows in the table |
| `error_pct` | Percentage of rows affected |

---

## Social data_CHECK_Silver XLS Formula Errors

**Notebook:** `TEAM > Social data_CHECK_silver xls formula errors`

Scans all tables in the silver schema for Excel error values (e.g. `#N/A`, `#DIV/0!`, `#VALUE!` etc.) and reports the count of errors by dataset, column and error type. Use this to verify whether Excel errors were successfully cleaned during the bronze to silver transfer, or if they are still present and causing rows to land in quarantine.

---

### 0. Config

```python
CATALOG = "team"
SILVER_SCHEMA = "silver"
```

---

### 1. Scan All Silver Tables for Excel Errors

Loops through every silver table (excluding quarantine tables), checks every string column for Excel error patterns in a single aggregation pass, and records any errors found.

```python
from pyspark.sql import functions as F

EXCEL_ERRORS = {
    "N/A":    r"^(ERROR:)?#N/A$",
    "DIV/0!": r"^(ERROR:)?#DIV/0!$",
    "VALUE!": r"^(ERROR:)?#VALUE!$",
    "REF!":   r"^(ERROR:)?#REF!$",
    "NAME?":  r"^(ERROR:)?#NAME\?$",
    "NULL!":  r"^(ERROR:)?#NULL!$",
    "NUM!":   r"^(ERROR:)?#NUM!$",
}

silver_tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").collect()
    if not r["tableName"].endswith("_quarantine")
]

results = []

for table_name in silver_tables:
    silver_path = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    df = spark.table(silver_path)
    total_rows = df.count()

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df_with_flags = df.select(
                *[
                    F.when(F.col(col_name).rlike(pattern), 1)
                     .otherwise(0)
                     .alias(error_type)
                    for error_type, pattern in EXCEL_ERRORS.items()
                ]
            )

            agg_exprs = [F.sum(error_type).alias(error_type) for error_type in EXCEL_ERRORS]
            agg_result = df_with_flags.agg(*agg_exprs).collect()[0]

            for error_type in EXCEL_ERRORS:
                error_count = agg_result[error_type]
                if error_count > 0:
                    results.append({
                        "dataset":           table_name,
                        "column":            col_name,
                        "column_type":       dtype,
                        "error_type":        error_type,
                        "excel_error_count": error_count,
                        "total_rows":        total_rows,
                        "error_pct":         round(error_count / total_rows * 100, 2),
                    })
```

---

### 2. Results

```python
if results:
    display(
        spark.createDataFrame(results)
        .orderBy("dataset", "column", "error_type")
    )
    print(f"\nTotal columns with Excel errors: {len(results)}")
else:
    print("No Excel errors found in any silver table — cleaning was successful.")
```

**Output columns:**

| Column | Description |
|---|---|
| `dataset` | Silver table name |
| `column` | Column containing the error |
| `column_type` | Data type of the column |
| `error_type` | Excel error type (e.g. `N/A`, `DIV/0!`) |
| `excel_error_count` | Number of cells with this error in this column |
| `total_rows` | Total rows in the table |
| `error_pct` | Percentage of rows affected |
