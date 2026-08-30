# Payment Pipeline - Technical Documentation

Lakeflow Declarative Pipeline covering Landing → Bronze → Silver → Gold for the Payment dataset.

---

## Architecture Overview

- **Catalog:** `team`
- **Landing volume:** `team.landing.payment` (subfolders: `historical/`, `incremental/`)
- **Bronze schema:** `team.bronze`
- **Reference data:** `team.bronze.partner_category_map` (hand-maintained, SQL-created, never written by the pipeline)
- **Orchestration:** single Lakeflow Declarative Pipeline (Python, `pyspark.pipelines` / `dp` API), **scheduled weekly for Friday 14:00 (Swiss time)**. Four transformation files; execution order is derived from data dependencies, not filenames
- **Source formats:** both modern `.xlsx` and legacy `.xls` (files up to ~20 years old that cannot be re-saved). Both are handled in-pipeline; format is detected from magic bytes, not extension.
- **Pipeline environment dependencies:** `openpyxl==3.1.*`, `xlrd==2.0.*` (must be added under Pipeline environment **and** applied via "Apply environment" - saving alone does not take effect)

---

## I) Original data storage, ingestion (LANDING → BRONZE)

### A) `total_sheet_partners_raw` -> `total_sheet_partners`

**Purpose:** build an authoritative, per-file list of real partner names from each workbook's Total Sheet. This is the backbone of partner resolution in stage 2.

**Why it exists.** Column 1 of every detail tab holds a mix of things that are structurally indistinguishable: the partner's name, a country continuation line (`Russia` beneath `NTV Plus`), free-text notes (`8-ung Royalties due within 30 business Days`), and `TOTAL` rows. Successive attempts to tell them apart from layout alone - blank `resp_code`, sheet-row adjacency, the installment column resetting to `1st` - each worked on the files they were built against and then failed on the next one. None of those signals is reliable across two decades of hand-maintained workbooks.

The Total Sheet settles it: one row per partner, with name and country in **separate** columns, and its cells are formulas referencing the detail tabs - so the strings are identical **by construction**, not by coincidence. Verified across files: every Total Sheet name exact-matches a detail tab name, so no fuzzy matching is needed, and none is done.

- Source: Auto Loader (`cloudFiles`, `binaryFile`) over `/Volumes/team/landing/payment/`, `pathGlobFilter = "*.xls*"`
- **Sheet selection:** `Total Sheet (Euro)` preferred, `Total Sheet (CHF)` as fallback, then any sheet starting `total sheet`. Both currencies list the same roster, so taking both would duplicate every partner
- **Row classification** is structural, not label-matching:
  * column 2 starts with `2.` -> stop parsing (the `2. Accounts Receivable Overview` section is a summary-of-summaries whose rows pass the numeric test but are aggregates)
  * column 1 empty, or `TOTAL` -> skip (section titles live in column 2, so they are excluded here)
  * all five money columns (4-8) numeric -> a real partner row
  * money columns hold header **text** instead -> a section header row; its column-1 label is captured verbatim
- **Columns captured (1-9):** `partner_name`, `partner_country`, `responsible`, `sales_contracted`, `invoiced`, `not_yet_invoiced`, `sales_paid`, `overdue`, `action`. Column 10+ is a currency-conversion helper and is not read
- **Category resolution** joins the verbatim `section_label` to `team.bronze.partner_category_map`, which carries **defaults and per-file overrides**. A mapping row with `file_pattern IS NULL` is a default applying to every workbook; a row with `file_pattern` set applies only where that string appears (case-insensitively) in the source file path. This exists because the same label genuinely means different things in different files: `PARTNERS` is New Media in UCL 06-07 but Licensees in UEL 13-14, and no single global mapping can express both
- **Override resolution:** a row may match several mapping entries, so candidates are ranked with a window function - overrides ahead of defaults, then longest `file_pattern` first as the most specific - and only the top-ranked match is kept. `(section_label, file_pattern)` must therefore be unique, which the setup script checks
- The join is **LEFT, never inner**: an unmapped label leaves `partner_category` NULL and is flagged, rather than silently removing the partner
- **Data quality (warn-only):** `has_partner_name`, `has_partner_category`, `has_competition`

**Known scope limitation.** The Total Sheet sections are titled *Accounts Receivable*, so a partner contracted but never invoiced can legitimately be absent (confirmed: `Kitbag Ltd`, UCL 13-14 Licensing, contracted 117, invoiced 0). This table is therefore authoritative for **names**, not a complete roster. Most such rows are filtered out downstream by the invoice-number rule anyway; where they are not, stage 2's `has_partner` expectation surfaces them. A small number of rows in UEL 13-14 Licensing remain unresolved on this basis and are treated as a documented exception rather than suppressed.

```python
import io
import re
from urllib.parse import unquote

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import openpyxl

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

# Same landing volume as the payment pipeline
LANDING_VOLUME_PATH = "/Volumes/team/landing/payment/"

TOTAL_SHEET_PREFERENCE = ["total sheet (euro)", "total sheet (chf)"]

COL_PARTNER_NAME = 1
COL_PARTNER_COUNTRY = 2
COL_RESPONSIBLE = 3
COL_SALES_CONTRACTED = 4
COL_INVOICED = 5
COL_NOT_YET_INVOICED = 6
COL_SALES_PAID = 7
COL_OVERDUE = 8
COL_ACTION = 9

# Every row that is a genuine partner has a number in all of these.
# Section headers have the header TEXT here instead, which is what makes
# this a reliable structural filter rather than a heuristic.
NUMERIC_COLS = (
    COL_SALES_CONTRACTED,
    COL_INVOICED,
    COL_NOT_YET_INVOICED,
    COL_SALES_PAID,
    COL_OVERDUE,
)


OVERVIEW_SECTION_PATTERN = re.compile(r"^\s*2\.")

# Competition + season from the file path, matching the payment pipeline's
# conventions so the two join on the same keys.
COMPETITION_PATTERN = re.compile(r"([A-Za-z]+)[ _]\d{2,4}-\d{2,4}[ _]Payments")
SEASON_PATTERN = re.compile(r"[A-Za-z]+[ _](\d{2,4}-\d{2,4})[ _]Payments")

PARTNER_STRUCT = StructType(
    [
        StructField("partner_name", StringType(), nullable=False),
        StructField("partner_country", StringType(), nullable=True),
        StructField("responsible", StringType(), nullable=True),
        StructField("sales_contracted", DoubleType(), nullable=True),
        StructField("invoiced", DoubleType(), nullable=True),
        StructField("not_yet_invoiced", DoubleType(), nullable=True),
        StructField("sales_paid", DoubleType(), nullable=True),
        StructField("overdue", DoubleType(), nullable=True),
        StructField("action", StringType(), nullable=True),
        StructField("section_label", StringType(), nullable=True),
        StructField("source_sheet", StringType(), nullable=False),
        StructField("sheet_row_number", IntegerType(), nullable=False),
    ]
)
PARSE_RESULT_SCHEMA = StructType(
    [StructField("partners", ArrayType(PARTNER_STRUCT), nullable=False)]
)

_decode_udf = F.udf(lambda p: unquote(p) if p else None, StringType())


def _clean_str(value):
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _parse_total_sheet(content: bytes):
    try:
        return _parse_total_sheet_inner(content)
    except Exception:
        return {"partners": []}


def _parse_total_sheet_inner(content: bytes):
    if content is None:
        return {"partners": []}

    head = bytes(content[:8])
    is_xls = head == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
    is_xlsx = head[:4] == b"PK\x03\x04"
    if not is_xls and not is_xlsx:
        return {"partners": []}

    # Normalise both readers to a common shape: a list of sheet names, and
    # a (row, col) -> value accessor using 1-indexed Excel coordinates.
    if is_xls:
        import xlrd

        book = xlrd.open_workbook(file_contents=bytes(content))
        sheet_names = book.sheet_names()

        def make_accessor(sheet_name):
            sheet = book.sheet_by_name(sheet_name)

            def get(r, c):
                if r > sheet.nrows or c > sheet.ncols:
                    return None
                cell = sheet.cell(r - 1, c - 1)
                value = cell.value
                if cell.ctype == xlrd.XL_CELL_EMPTY:
                    return None
                if cell.ctype == xlrd.XL_CELL_DATE:
                    return xlrd.xldate_as_datetime(value, book.datemode)
                return value

            return get, sheet.nrows
    else:
        wb = openpyxl.load_workbook(io.BytesIO(bytes(content)), data_only=True)
        sheet_names = wb.sheetnames

        def make_accessor(sheet_name):
            ws = wb[sheet_name]

            def get(r, c):
                return ws.cell(row=r, column=c).value

            return get, ws.max_row

    # Pick exactly one Total Sheet: Euro if present, else CHF. Both carry
    # the same partner roster, so taking both would duplicate every row.
    chosen = None
    lowered = {s.lower().strip(): s for s in sheet_names}
    for preferred in TOTAL_SHEET_PREFERENCE:
        if preferred in lowered:
            chosen = lowered[preferred]
            break
    if chosen is None:
        # Fall back to any sheet whose name starts with "total sheet",
        # in case a file uses a currency we have not seen before.
        for low, original in lowered.items():
            if low.startswith("total sheet"):
                chosen = original
                break
    if chosen is None:
        return {"partners": []}

    get, max_row = make_accessor(chosen)

    partners = []
    current_section_label = None
    for r in range(1, max_row + 1):
        # Stop at the Overview section - everything below is aggregates.
        section_title = get(r, 2)
        if isinstance(section_title, str) and OVERVIEW_SECTION_PATTERN.match(section_title):
            break

        name = _clean_str(get(r, COL_PARTNER_NAME))

        if not name or name.upper() == "TOTAL":
            continue

        numbers = [get(r, c) for c in NUMERIC_COLS]
        is_partner_row = all(isinstance(v, (int, float)) for v in numbers)

        if not is_partner_row:
            header_text = [get(r, c) for c in NUMERIC_COLS]
            looks_like_header = any(isinstance(v, str) and v.strip() for v in header_text)
            if looks_like_header:
                current_section_label = name
            continue

        partners.append(
            {
                "partner_name": name,
                "partner_country": _clean_str(get(r, COL_PARTNER_COUNTRY)),
                "responsible": _clean_str(get(r, COL_RESPONSIBLE)),
                "sales_contracted": float(numbers[0]),
                "invoiced": float(numbers[1]),
                "not_yet_invoiced": float(numbers[2]),
                "sales_paid": float(numbers[3]),
                "overdue": float(numbers[4]),
                "action": _clean_str(get(r, COL_ACTION)),
                "section_label": current_section_label,
                "source_sheet": chosen,
                "sheet_row_number": r,
            }
        )

    return {"partners": partners}


_parse_udf = F.udf(_parse_total_sheet, PARSE_RESULT_SCHEMA)


@dp.table(
    name="total_sheet_partners_raw",
    comment=(
        "Landing raw: one row per partner as listed in each workbook's "
        "Total Sheet (Euro preferred, CHF fallback). Structural extraction "
        "only. Carries _source_file_modified_at (ADDING INCREMENTAL "
        "DOWNLOAD CHECK) so total_sheet_partners can determine which "
        "uploaded file is current per competition/season."
    ),
)
def total_sheet_partners_raw():
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xls*")
        .load(LANDING_VOLUME_PATH)
        # ============ ADDING INCREMENTAL DOWNLOAD CHECK ============
        # Cloud storage last-modified time, captured immediately after the
        # read (before select/explode, since _metadata may not resolve
        # reliably several transforms later).
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        # ============================================================
    )

    parsed = raw.withColumn("_parsed", _parse_udf(F.col("content")))
    exploded = parsed.select(
        "path",
        "_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
        F.explode(F.col("_parsed.partners")).alias("_p"),
    )

    return (
        exploded.select(
            F.col("_p.partner_name").alias("partner_name"),
            F.col("_p.partner_country").alias("partner_country"),
            F.col("_p.responsible").alias("responsible"),
            F.col("_p.sales_contracted").alias("sales_contracted"),
            F.col("_p.invoiced").alias("invoiced"),
            F.col("_p.not_yet_invoiced").alias("not_yet_invoiced"),
            F.col("_p.sales_paid").alias("sales_paid"),
            F.col("_p.overdue").alias("overdue"),
            F.col("_p.action").alias("action"),
            F.col("_p.section_label").alias("section_label"),
            F.col("_p.source_sheet").alias("_source_sheet"),
            F.col("_p.sheet_row_number").alias("_sheet_row_number"),
            "_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
            "path",
        )
        .withColumn("_source_file", _decode_udf(F.col("path")))
        # ADDING INCREMENTAL DOWNLOAD CHECK
        .withColumnRenamed("_file_modified_at", "_source_file_modified_at")
        .withColumn(
            "_ingested_at",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )
        .drop("path")
    )


@dp.table(
    name="total_sheet_partners",
    comment=(
        "One row per (source file, partner), from that workbook's Total "
        "Sheet. Authoritative for partner NAMES: names here exact-match the "
        "detail tabs by construction, since Total Sheet cells are formulas "
        "referencing them. Matching downstream MUST be scoped to the same "
        "_source_file - names can differ between seasons (confirmed: "
        "'British  Sky BC' with two spaces in one file, one space in "
        "another), so this is never deduplicated across files. "
        "ADDING INCREMENTAL DOWNLOAD CHECK: filtered to the most recently "
        "modified source file per (competition, season) - superseded "
        "uploads for the same season remain in total_sheet_partners_raw as "
        "history but are excluded here."
    ),
)
@dp.expect("has_partner_name", "partner_name IS NOT NULL")
@dp.expect("has_partner_category", "partner_category IS NOT NULL")
@dp.expect("has_competition", "competition IS NOT NULL AND competition <> ''")
def total_sheet_partners():
    df = spark.read.table("total_sheet_partners_raw")  # noqa: F821

    # A default row (file_pattern IS NULL) applies to any file; an override
    # (file_pattern set) applies only where the source path contains it.
    # Overrides beat defaults, and where several match, the longest pattern
    # wins as the most specific.
    category_map = spark.read.table("team.bronze.partner_category_map")  # noqa: F821
    category_map = category_map.select(
        F.upper(F.trim(F.col("section_label"))).alias("_map_label"),
        F.upper(F.trim(F.col("file_pattern"))).alias("_map_file_pattern"),
        F.col("partner_category").alias("_mapped_category"),
    )

    joined = df.join(
        category_map,
        (F.upper(F.trim(F.col("section_label"))) == F.col("_map_label"))
        & (
            F.col("_map_file_pattern").isNull()
            | F.upper(F.col("_source_file")).contains(F.col("_map_file_pattern"))
        ),
        "left",
    )

    # Rank candidates per row: overrides first, then by pattern length
    # descending. Row 1 is the winner.
    specificity = Window.partitionBy(
        "_source_file", "_source_sheet", "_sheet_row_number", "partner_name"
    ).orderBy(
        F.col("_map_file_pattern").isNull().asc(),
        F.length(F.col("_map_file_pattern")).desc_nulls_last(),
    )

    df = (
        joined.withColumn("_map_rank", F.row_number().over(specificity))
        .filter(F.col("_map_rank") == 1)
        .withColumnRenamed("_mapped_category", "partner_category")
        .drop("_map_label", "_map_file_pattern", "_map_rank")
    )

    df = (
        df.withColumn(
            "competition",
            F.regexp_extract(F.col("_source_file"), COMPETITION_PATTERN.pattern, 1),
        )
        .withColumn(
            "season",
            F.regexp_extract(F.col("_source_file"), SEASON_PATTERN.pattern, 1),
        )
        .withColumn(
            "partner_key",
            F.concat_ws(
                "|",
                F.regexp_extract(F.col("_source_file"), r"([^/]+)\.[Xx][Ll][Ss][Xx]?$", 1),
                F.col("section_label"),
                F.col("partner_name"),
            ),
        )
    )

    # ================= ADDING INCREMENTAL DOWNLOAD CHECK =================
    # Only trust the most recently modified file per (competition, season).
    # dense_rank (not row_number) is required: every row belonging to the
    # same source file shares the same _source_file_modified_at value, so
    # dense_rank correctly keeps ALL of that file's rows, where row_number
    # would arbitrarily cut the winning file off partway through.
    latest_file_window = Window.partitionBy("competition", "season").orderBy(
        F.col("_source_file_modified_at").desc()
    )
    df = (
        df.withColumn("_file_rank", F.dense_rank().over(latest_file_window))
        .filter(F.col("_file_rank") == 1)
        .drop("_file_rank")
    )
    # =======================================================================

    return df.select(
        "partner_key",
        "partner_name",
        "partner_country",
        "partner_category",
        "section_label",
        "responsible",
        "competition",
        "season",
        "sales_contracted",
        "invoiced",
        "not_yet_invoiced",
        "sales_paid",
        "overdue",
        "action",
        "_source_file",
        "_source_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
        "_source_sheet",
        "_sheet_row_number",
        "_ingested_at",
    )

```

---

### B) `<category>_raw` (Streaming Tables)

**Purpose:** structural extraction from the detail payment tabs. No business logic.

- Source: Auto Loader (`cloudFiles`) streaming read from `/Volumes/team/landing/payment/`, recursively covering `historical/` and `incremental/` in a single stream
- Format: `cloudFiles.format = "binaryFile"` - files arrive as raw bytes and are parsed in a Python UDF. Auto Loader has no native Excel reader here, unlike the DCB pipeline
- **No `cloudFiles.schemaLocation` is set.** Lakeflow manages schema and checkpoint state internally for pipeline-owned streaming tables; specifying a path caused `UC_VOLUME_NOT_FOUND` against a volume that should never have needed to exist
- **Tab matching is dynamic**, per the original brief ("select tabs that have 'TV Payment' in its name"): each category is a case-insensitive regex against every sheet name in the workbook. A file may contribute 0, 1 or several matching tabs - the UCL files carry both `TV PAYMENTS` and `TV PAYMENTS EX`, and both flow into `tv_payments_raw`, tagged with `_source_tab` so nothing is lost
- **Dual-format reader:** `.xlsx` (ZIP, `PK\x03\x04`) -> openpyxl; `.xls` (OLE2, `\xd0\xcf\x11\xe0...`) -> xlrd. Detected from **magic bytes rather than extension**, so a mislabelled file still routes correctly. Files matching neither signature are skipped rather than passed to openpyxl as a fallback
- **xlrd normalisation:** dates converted to datetime (xlrd returns floats plus a type flag) and integral floats coerced to int, so `12345.0` and `12345` do not diverge between readers and stage 2 sees identical input regardless of source format
- **Whole-file try/except:** one unreadable workbook contributes zero rows instead of failing the stream. A failed streaming table is not committed at all, which previously cascaded into stage 2 failing with `TABLE_OR_VIEW_NOT_FOUND`
- Extraction: row 6 onward, columns 1-24, every cell stringified. Columns renamed **positionally** (`col_1` ... `col_24`) - source header text is never trusted
- Metadata: `_source_tab`, `_sheet_row_number` (the true Excel row, captured during parsing rather than reconstructed from explode position), `_source_file` (URL-decoded), `_ingested_at`, `_season_from_filename`

```python
import io
import re
from urllib.parse import unquote

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import openpyxl

# -----------------------------------------------------------------------------
# CONFIG - confirm / adjust before first run
# -----------------------------------------------------------------------------

LANDING_VOLUME_PATH = "/Volumes/team/landing/payment/"

# Row 6 is the interim header in every source tab. Rows 1-5 are
# title/report metadata and get dropped here.
INTERIM_HEADER_ROW = 6  # 1-indexed, Excel convention

N_COLS = 24  # capped: confirmed identical business meaning across every
             # matching tab from column 10 onward, 1:1 for columns 1-9
             # once cross-checked against each tab's detail block. Columns
             # 25+ are out of scope entirely.

CATEGORY_CONFIGS = [
    {"raw_table": "tv_payments_raw", "pattern": re.compile(r"tv\s*payment", re.IGNORECASE)},
    {"raw_table": "sponsor_payments_raw", "pattern": re.compile(r"sponsor\s*payment", re.IGNORECASE)},
    {"raw_table": "licensing_payments_raw", "pattern": re.compile(r"licensing\s*payment", re.IGNORECASE)},
]

# Filename -> season metadata. Competition prefix is intentionally
# generic ([A-Za-z]+), not hardcoded to UCL, so this keeps working
# as new competitions are added (e.g. UECL) without another code
# change - same approach already used by COMPETITION_PATTERN in the
# stage-2 transform file. Confirmed against real filenames:
#   "UCL 2010-2011 Payments.xlsx", "UCL 17-18 Payments.xlsx",
#   "UCL 24-25 Payments.xlsx", "UCL 06-07 Payments - NHU.xlsx",
#   "UEL 13-14 Payments.xlsx", "UEL 21-22 Payments.xlsx"
FILENAME_METADATA_PATTERN = re.compile(
    r"[A-Za-z]+[ _](?P<season>\d{2,4}-\d{2,4})[ _]Payments", re.IGNORECASE
)

ROW_STRUCT = StructType(
    [
        StructField("sheet_name", StringType(), nullable=False),
        StructField("sheet_row_number", IntegerType(), nullable=False),
        StructField("cells", ArrayType(StringType()), nullable=False),
    ]
)
PARSE_RESULT_SCHEMA = StructType(
    [StructField("rows", ArrayType(ROW_STRUCT), nullable=False)]
)

_decode_udf = F.udf(lambda p: unquote(p) if p else None, StringType())
_season_udf = F.udf(
    lambda p: (
        m.group("season") if (m := FILENAME_METADATA_PATTERN.search(p or "")) else None
    ),
    StringType(),
)


# -----------------------------------------------------------------------------
# Shared parsing logic
# -----------------------------------------------------------------------------

def _make_category_parser(name_pattern: "re.Pattern"):

    def _parse(content: bytes):
        try:
            return _parse_inner(content)
        except Exception:
            return {"rows": []}

    def _parse_inner(content: bytes):
        if content is None:
            return {"rows": []}

        head = bytes(content[:8])
        is_xls = head == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
        is_xlsx = head[:4] == b"PK\x03\x04"

        if not is_xls and not is_xlsx:
            return {"rows": []}

        rows_out = []

        if is_xls:
            import xlrd

            book = xlrd.open_workbook(file_contents=bytes(content))
            matching_sheets = [s for s in book.sheet_names() if name_pattern.search(s)]
            for sheet_name in matching_sheets:
                sheet = book.sheet_by_name(sheet_name)
                # xlrd is 0-indexed; INTERIM_HEADER_ROW is the 1-indexed
                # Excel row number, so subtract 1 to get the start index.
                for r0 in range(INTERIM_HEADER_ROW - 1, sheet.nrows):
                    row_vals = []
                    any_value = False
                    for c0 in range(N_COLS):
                        if c0 < sheet.ncols:
                            cell = sheet.cell(r0, c0)
                            v = cell.value
                            if cell.ctype == xlrd.XL_CELL_DATE:
                                v = xlrd.xldate_as_datetime(v, book.datemode)
                            elif cell.ctype == xlrd.XL_CELL_EMPTY:
                                v = None
                            elif cell.ctype == xlrd.XL_CELL_NUMBER:
                                if v == int(v):
                                    v = int(v)
                        else:
                            v = None
                        if v is not None and v != "":
                            any_value = True
                            row_vals.append(str(v))
                        else:
                            row_vals.append(None)
                    if any_value:
                        # Report the 1-indexed Excel row number, matching
                        # the .xlsx branch.
                        rows_out.append(
                            {
                                "sheet_name": sheet_name,
                                "sheet_row_number": r0 + 1,
                                "cells": row_vals,
                            }
                        )
        else:
            wb = openpyxl.load_workbook(io.BytesIO(bytes(content)), data_only=True)
            matching_sheets = [s for s in wb.sheetnames if name_pattern.search(s)]
            for sheet_name in matching_sheets:
                ws = wb[sheet_name]
                for r in range(INTERIM_HEADER_ROW, ws.max_row + 1):
                    row_vals = []
                    any_value = False
                    for c in range(1, N_COLS + 1):
                        v = ws.cell(row=r, column=c).value
                        if v is not None:
                            any_value = True
                        row_vals.append(None if v is None else str(v))
                    if any_value:
                        rows_out.append(
                            {
                                "sheet_name": sheet_name,
                                "sheet_row_number": r,
                                "cells": row_vals,
                            }
                        )

        return {"rows": rows_out}

    return F.udf(_parse, PARSE_RESULT_SCHEMA)


def _build_raw_table(raw_table: str, name_pattern: "re.Pattern"):
    parser_udf = _make_category_parser(name_pattern)

    @dp.table(
        name=raw_table,
        comment=(
            f"Bronze raw: structural extraction of every sheet matching "
            f"'{name_pattern.pattern}' from landing. Positional columns "
            f"only, no header/type resolution yet. ADDING INCREMENTAL "
            f"DOWNLOAD CHECK: carries _source_file_modified_at (cloud "
            f"storage last-modified time) so downstream stage-2 tables can "
            f"determine which uploaded file is current per season, without "
            f"relying on filename conventions or manual deletion of "
            f"superseded files."
        ),
    )
    def _tbl():
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("pathGlobFilter", "*.xls*")
            .load(LANDING_VOLUME_PATH)
            # ============ ADDED INCREMENTAL DOWNLOAD CHECK ============
            # Captured immediately after the read, before any
            # select/explode, since _metadata may not resolve reliably
            # several transforms later.
            .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
            # ============================================================
        )

        parsed = raw.withColumn("_parsed", parser_udf(F.col("content")))
        exploded = parsed.select(
            "path",
            "_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
            F.explode(F.col("_parsed.rows")).alias("_row"),
        )

        # Positional column rename: col_1 .. col_N_COLS. Deliberately NOT
        # trusting source header text (inconsistent across files/tabs).
        col_exprs = [F.col("_row.cells")[i].alias(f"col_{i + 1}") for i in range(N_COLS)]

        result = (
            exploded.select(
                "path",
                "_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
                F.col("_row.sheet_name").alias("_source_tab"),
                F.col("_row.sheet_row_number").alias("_sheet_row_number"),
                *col_exprs,
            )
            .withColumn("_source_file", _decode_udf(F.col("path")))
            # ADDING INCREMENTAL DOWNLOAD CHECK
            .withColumnRenamed("_file_modified_at", "_source_file_modified_at")
            .withColumn(
                "_ingested_at",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            )
            .withColumn("_season_from_filename", _season_udf(F.col("_source_file")))
            .drop("path")
        )

        return result

    return _tbl


# -----------------------------------------------------------------------------
# Register one streaming table per category
# -----------------------------------------------------------------------------

for cfg in CATEGORY_CONFIGS:
    _build_raw_table(cfg["raw_table"], cfg["pattern"])

```

---

### C) `<category>` (Materialized Views)

**Purpose:** typing, row filtering, and partner resolution. Reads `<category>_raw` as a **batch** source - required because the window functions below are unsupported on streaming DataFrames.

- **Invoice cleaning and cast:** the leading digit run is extracted before `try_cast(... as bigint)`, so a value like `112858/882` (two combined invoice numbers) yields `112858` rather than nulling out and losing a real row
- **Single row filter:** `invoice_number IS NOT NULL`. This one condition removes the interim header band, every repeated mid-sheet header block, all `TOTAL` rows, blank separators, and the entire pre-detail summary section. `invoice_number` is the **only** reliable signal for whether a row is real data - the installment column is not, having been proven to repeat `1st` across 2-4 rows for a single invoice when a payment is split across territories
- **Partner resolution against `total_sheet_partners`,** scoped to the same `_source_file` (names drift between seasons - `British  Sky BC` with two spaces in one file, one in another). A column-1 value that matches is a partner and seeds a block; a value that does not is noise and is nulled, inheriting the partner of the block it sits in
- **Fan-out guard:** the lookup is reduced with `collect_set` to strictly one row per (file, name) before joining. A name can legitimately appear several times in one Total Sheet - a broadcaster with contracts in both the Europe and Ex-Europe sections - and a naive join would silently multiply payment rows
- **`partner_category` comes from the lookup, not from the tab.** The Total Sheet has better context: it separates suppliers from sponsors, which tab names do not. The tab's own category is used **only** to break a genuine tie, where a name is listed under two different categories in the same file (confirmed: `ESPN` as both broadcaster and licensee, `adidas` as both licensee and supplier, in UCL 2010-2011). This is why the taxonomy in `partner_category_map` must stay aligned with the literals passed to `_transform`
- **Block ID and forward-fill:** `_block_id` increments on each row that seeded a real partner; `partner`, `partner_category` and `resp_code` are then carried down through each block via `last(..., ignorenulls=True)`
- **Window partitioning is `(_source_file, _source_tab)`, never `_source_file` alone** - one file can contain two matching tabs for the same category whose `_sheet_row_number` values legitimately overlap, and EU/EX rows would otherwise bleed into each other's fill blocks
- **Explicit typing** against a hardcoded 24-column name and type list, applied *after* the row filter so the ~60% of rows that are structural noise are never cast. All casts are `try_cast`
- **Row ID:** `filename - tab - block - sheet row`, including the tab so it stays unique when two tabs from one file share a block and row number
- **Data quality (warn-only, never `expect_or_drop`):** `has_partner`, `category_not_from_tab_fallback`

**Column naming caveat.** Names are hardcoded rather than derived, and two of them contradict their own header text: column 6 (`Invoiced`) holds an invoicing **date**, and column 8 (labelled `not yet invoiced` at row 6) holds a **due date**. Both were confirmed against the detail block headers and the actual surviving data, not the interim header row.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.pipelines as dp

COMPETITION_PATTERN = r"([A-Za-z]+)[ _]\d{2,4}-\d{2,4}[ _]Payments"

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

N_COLS = 24

COL_PARTNER = "col_1"          # broadcaster / sponsor / licensee name
COL_RESP_CODE = "col_2"        # responsible-team code (SDT/PDM, TLI, ...)
COL_INVOICE = "col_7"          # Invoice Number

# Hardcoded column names, position 1-24. Confirmed identical across every
# matching tab in every category - see stage 1 for how tabs are matched.
COLUMN_NAMES = [
    "partner",                    # 1 - superseded by block-filled `partner` below
    "resp_code",                  # 2 - superseded by block-filled `resp_code` below
    "installments",                # 3
    "currency",                    # 4
    "sales_contracted_original",   # 5
    "invoiced_original",           # 6
    "invoice_number",              # 7 - superseded by cleaned/cast `invoice_number` below
    "due_on",                      # 8 - NOT "not yet invoiced" (row 6's label) - 
                                    #      real data in this column is a due date
    "receipt_of_payment",          # 9
    "sales_paid_original",         # 10
    "overdue_original",            # 11
    "action",                      # 12
    "x_rate_chf",                   # 13
    "sales_contracted_chf",         # 14
    "invoiced_chf",                 # 15
    "not_yet_invoiced_chf",         # 16
    "sales_paid_chf",               # 17
    "overdue_chf",                  # 18
    "x_rate_eur",                    # 19
    "sales_contracted_eur",          # 20
    "invoiced_eur",                  # 21
    "not_yet_invoiced_eur",          # 22
    "sales_paid_eur",                # 23
    "overdue_eur",                   # 24
]
assert len(COLUMN_NAMES) == N_COLS

COLUMN_TYPES = {
    5: "double",           # sales_contracted_original
    6: "date",              # invoiced_original - date of invoicing, not
    8: "date",              # due_on
    9: "date",              # receipt_of_payment
    10: "double",           # sales_paid_original
    11: "double",           # overdue_original
    13: "decimal(18,6)",    # x_rate_chf
    14: "double",           # sales_contracted_chf
    15: "double",           # invoiced_chf
    16: "double",           # not_yet_invoiced_chf
    17: "double",           # sales_paid_chf
    18: "double",           # overdue_chf
    19: "decimal(18,6)",    # x_rate_eur
    20: "double",           # sales_contracted_eur
    21: "double",           # invoiced_eur
    22: "double",           # not_yet_invoiced_eur
    23: "double",           # sales_paid_eur
    24: "double",           # overdue_eur
}


# -----------------------------------------------------------------------------
# Shared transform (takes/returns a DataFrame - no table names inside)
# -----------------------------------------------------------------------------

def _transform(df, lookup, tab_category: str):
    # ================= ADDING INCREMENTAL DOWNLOAD CHECK =================
    # Only trust the most recently modified file per (competition, season).
    # <category>_raw is append-only (Auto Loader never forgets a file, and
    # never notices one was deleted from landing), so without this filter
    # a corrected re-upload for a season already in the table would
    # silently double-count every invoice in that season rather than
    # replacing the superseded file's rows.
    #
    # competition is computed here (moved up from later in this function,
    # where the original code computed it a second time right before
    # rename_exprs - that duplicate computation has been removed since
    # this one now covers it) because the ranking below needs it.
    #
    # dense_rank (not row_number) is required: every row from one file
    # shares the same _source_file_modified_at, so dense_rank correctly
    # keeps ALL of the winning file's rows across every matching tab,
    # where row_number would arbitrarily cut it off partway through.
    df = df.withColumn(
        "competition",
        F.regexp_extract(F.col("_source_file"), COMPETITION_PATTERN, 1),
    )
    latest_file_window = Window.partitionBy(
        "competition", "_season_from_filename"
    ).orderBy(F.col("_source_file_modified_at").desc())
    df = (
        df.withColumn("_file_rank", F.dense_rank().over(latest_file_window))
        .filter(F.col("_file_rank") == 1)
        .drop("_file_rank")
    )
    # =======================================================================

    df = df.withColumn(
        COL_INVOICE,
        F.expr(f"try_cast(regexp_extract(trim({COL_INVOICE}), '^([0-9]+)', 1) as bigint)"),
    )
    df = df.filter(F.col(COL_INVOICE).isNotNull())

    lookup_by_name = (
        lookup.groupBy(
            F.col("_source_file").alias("_lk_file"),
            F.upper(F.trim(F.col("partner_name"))).alias("_lk_name"),
        )
        .agg(
            F.collect_set(F.col("partner_category")).alias("_lk_categories"),
            F.first(F.col("partner_name"), ignorenulls=True).alias("_lk_canonical_name"),
        )
        .withColumn("_lk_category_count", F.size(F.col("_lk_categories")))
    )

    df = df.join(
        lookup_by_name,
        (F.col("_source_file") == F.col("_lk_file"))
        & (F.upper(F.trim(F.col(COL_PARTNER))) == F.col("_lk_name")),
        "left",
    )

    # A row seeds a new partner block only if its column-1 value matched a
    # real partner. Everything else - countries, notes, blanks - is nulled
    # and will inherit the partner of the block it sits in.
    matched = F.col("_lk_canonical_name").isNotNull()
    df = df.withColumn(
        "_partner_seed",
        F.when(matched, F.col("_lk_canonical_name")).otherwise(F.lit(None).cast("string")),
    )

    df = df.withColumn(
        "_category_seed",
        F.when(~matched, F.lit(None).cast("string"))
        .when(F.col("_lk_category_count") == 1, F.element_at(F.col("_lk_categories"), 1))
        .otherwise(F.lit(tab_category)),
    )

    # Flag for the ambiguous case above, so it stays visible rather than
    # quietly resolving to the tab's answer forever.
    df = df.withColumn(
        "_category_from_tab_fallback",
        matched & (F.col("_lk_category_count") > 1),
    )

    df = df.drop("_lk_file", "_lk_name", "_lk_categories", "_lk_canonical_name")

    order_win = Window.partitionBy("_source_file", "_source_tab").orderBy("_sheet_row_number")
    block_start = F.when(F.col("_partner_seed").isNotNull(), 1).otherwise(0)
    df = df.withColumn(
        "_block_id",
        F.sum(block_start).over(order_win.rowsBetween(Window.unboundedPreceding, 0)),
    )

    # Forward-fill partner, category and resp_code down through each block.
    fill_win = order_win.rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("partner", F.last("_partner_seed", ignorenulls=True).over(fill_win))
    df = df.withColumn(
        "partner_category", F.last("_category_seed", ignorenulls=True).over(fill_win)
    )
    df = df.withColumn(
        "resp_code", F.last(COL_RESP_CODE, ignorenulls=True).over(fill_win)
    )

    # Human-readable row id - includes the tab name so it stays unique
    # even when two tabs from one file share a block and row number.
    df = df.withColumn(
        "row_id",
        F.concat_ws(
            "-",
            F.regexp_extract(F.col("_source_file"), r"([^/]+)\.xlsx$", 1),
            F.regexp_replace(F.col("_source_tab"), r"\s+", "_"),
            F.col("_block_id").cast("string"),
            F.col("_sheet_row_number").cast("string"),
        ),
    )

    rename_exprs = []
    for i in range(N_COLS):
        raw_col = f"col_{i + 1}"
        if raw_col in (COL_PARTNER, COL_RESP_CODE):
            continue
        target_name = "invoice_number" if raw_col == COL_INVOICE else COLUMN_NAMES[i]
        col_position = i + 1
        if col_position in COLUMN_TYPES:
            spark_type = COLUMN_TYPES[col_position]
            expr = F.expr(f"try_cast(trim({raw_col}) as {spark_type})").alias(target_name)
        else:
            expr = F.col(raw_col).alias(target_name)
        rename_exprs.append(expr)

    return df.select(
        "row_id",
        "partner",
        "resp_code",
        "partner_category",
        "_category_from_tab_fallback",
        "competition",
        "_block_id",
        "_sheet_row_number",
        "_source_tab",
        "_source_file",
        "_source_file_modified_at",  # ADDING INCREMENTAL DOWNLOAD CHECK
        "_ingested_at",
        "_season_from_filename",
        *rename_exprs,
    )


@dp.table(
    name="tv_payments",
    comment=(
        "Bronze: typed, header-named TV payment data. Partner names and "
        "categories are resolved against total_sheet_partners, per source "
        "file. ADDING INCREMENTAL DOWNLOAD CHECK: filtered to the most "
        "recently modified file per (competition, season) - superseded "
        "uploads remain in tv_payments_raw as history but are excluded "
        "here."
    ),
)
@dp.expect("has_partner", "partner IS NOT NULL")
@dp.expect("category_not_from_tab_fallback", "_category_from_tab_fallback = false")
def tv_payments():
    df = spark.read.table("tv_payments_raw")  # noqa: F821
    lookup = spark.read.table("total_sheet_partners")  # noqa: F821
    return _transform(df, lookup, "BROADCASTER")


@dp.table(
    name="sponsor_payments",
    comment=(
        "Bronze: typed, header-named Sponsor payment data. Partner names "
        "and categories are resolved against total_sheet_partners, per "
        "source file. ADDING INCREMENTAL DOWNLOAD CHECK: filtered to the "
        "most recently modified file per (competition, season) - "
        "superseded uploads remain in sponsor_payments_raw as history but "
        "are excluded here."
    ),
)
@dp.expect("has_partner", "partner IS NOT NULL")
@dp.expect("category_not_from_tab_fallback", "_category_from_tab_fallback = false")
def sponsor_payments():
    df = spark.read.table("sponsor_payments_raw")  # noqa: F821
    lookup = spark.read.table("total_sheet_partners")  # noqa: F821
    return _transform(df, lookup, "SPONSOR")


@dp.table(
    name="licensing_payments",
    comment=(
        "Bronze: typed, header-named Licensing payment data. Partner names "
        "and categories are resolved against total_sheet_partners, per "
        "source file. ADDING INCREMENTAL DOWNLOAD CHECK: filtered to the "
        "most recently modified file per (competition, season) - "
        "superseded uploads remain in licensing_payments_raw as history "
        "but are excluded here."
    ),
)
@dp.expect("has_partner", "partner IS NOT NULL")
@dp.expect("category_not_from_tab_fallback", "_category_from_tab_fallback = false")
def licensing_payments():
    df = spark.read.table("licensing_payments_raw")  # noqa: F821
    lookup = spark.read.table("total_sheet_partners")  # noqa: F821
    return _transform(df, lookup, "LICENSEE")

```

---

### D) `payment` (Materialized View)

**Purpose:** harmonize the three category tables into the single table downstream consumers read.

- Pure union, no per-row computation. `partner_category` and `competition` are already set in stage 2
- `unionByName` in its default strict mode, deliberately: if a future edit to one stage-2 table drifts from the shared schema, this fails loudly rather than producing silently misaligned columns
- **Data quality (warn-only):** `has_partner`, `has_invoice_number` - both already guaranteed upstream, declared again here as defence in depth against the union itself, and so that anyone monitoring `payment` directly need not trace back through three separate tables

```python
import pyspark.pipelines as dp


@dp.table(
    name="payment",
    comment=(
        "Harmonized bronze payment table - union of TV, Sponsor, and "
        "Licensing. partner_category and competition are both already "
        "set per source table in stage 2."
    ),
)
@dp.expect("has_partner", "partner IS NOT NULL")
@dp.expect("has_invoice_number", "invoice_number IS NOT NULL")
def payment():
    tv = spark.read.table("tv_payments")  # noqa: F821
    sponsor = spark.read.table("sponsor_payments")  # noqa: F821
    licensing = spark.read.table("licensing_payments")  # noqa: F821

    return tv.unionByName(sponsor).unionByName(licensing)
```

## II) Data standardisation (BRONZE → SILVER) `payment` (Materialized View)

Purpose: trim bronze.payment to business-relevant columns and enrich with a cleaned, cross-season partner name.

- Column trim: drops 20 columns (lineage detail, original-currency and CHF blocks, rate columns) down to 13 - the business columns plus `_source_file`, `_ingested_at`, `_season_from_filename`. EUR figures only.
- `partner_cleaned` is added via a left join to `team.dim.umcc_partners` - a hand-maintained dimension, never written by the pipeline, read fully-qualified (same convention as `team.bronze.partner_category_map`). See `CREATE_payment_silver_DIM_umcc_partner.sql` for its DDL and data.
- The join compares an apostrophe-stripped version of `partner` on both sides, not the raw column directly. Confirmed case: `"Beijing XIN'AI Sports"` failed to match on an apostrophe-related mismatch; stripping it from both sides at join time (without altering either stored value) fixes that and any future name with the same issue.
- `umcc_partners` grain is `partner` alone, not `(partner, partner_category)`. Manual review of the full partner list confirmed no raw name maps to more than one `partner_cleaned` value, including names that legitimately span multiple categories (e.g. `adidas` as SPONSOR/SUPPLIER/LICENSEE all clean to the same name).
- Left join, never inner: an unmatched partner keeps its row with `partner_cleaned = NULL` rather than being dropped.
- `partner_country` was considered (live join to `total_sheet_partners`, scoped by `(partner, _source_file)`) and deliberately dropped - added no value on top of `partner_category`/`partner_cleaned` for this table.
- Data quality (warn-only): `has_invoice_number`, `has_partner_cleaned` - the latter is the signal to extend `umcc_partners`.
- Companion table `umcc_partners_quality_check`: a standalone read of `team.dim.umcc_partners` grouped by `partner`, flagging any raw value that appears more than once in the dim (which would fan out the join above).

```python
"""Silver transformation: team.silver.payment"""

import pyspark.pipelines as dp
from pyspark.sql import functions as F

SILVER_PAYMENT_COLUMNS = [
    "partner",
    "partner_category",
    "competition",
    "invoice_number",
    "invoiced_original",
    "due_on",
    "receipt_of_payment",
    "sales_contracted_eur",
    "invoiced_eur",
    "sales_paid_eur",
    "_source_file",
    "_ingested_at",
    "_season_from_filename",
]


def _strip_apostrophe(col):
    return F.regexp_replace(col, "'", "")


@dp.table(
    name="team.silver.payment",
    comment=(
        "Bronze payment trimmed to business-relevant columns, enriched with "
        "partner_cleaned from the hand-maintained team.dim.umcc_partners."
    ),
)
@dp.expect("has_invoice_number", "invoice_number IS NOT NULL")
@dp.expect("has_partner_cleaned", "partner_cleaned IS NOT NULL")
def payment():
    bronze_payment = spark.read.table("payment").select(*SILVER_PAYMENT_COLUMNS)
    bronze_payment = bronze_payment.withColumn(
        "_join_key", _strip_apostrophe(F.col("partner"))
    )

    umcc_partners_dim = spark.read.table("team.dim.umcc_partners").select(
        _strip_apostrophe(F.col("partner")).alias("_join_key"),
        "partner_cleaned",
    )

    return bronze_payment.join(
        umcc_partners_dim, on="_join_key", how="left"
    ).drop("_join_key")


@dp.table(
    name="team.silver.umcc_partners_quality_check",
    comment=(
        "Uniqueness check over team.dim.umcc_partners - flags duplicate "
        "partner values before they fan out the silver.payment join."
    ),
)
@dp.expect("partner_unique_in_dim", "match_count = 1")
def umcc_partners_quality_check():
    return (
        spark.read.table("team.dim.umcc_partners")
        .groupBy("partner")
        .agg(F.count("*").alias("match_count"))
    )
```

## III) Business readiness (SILVER → GOLD) `payment` (Materialized View)

Purpose: business-renamed, analysis-ready payment table with payment-timeliness scoring, per invoice and per partner-season. Rows with no resolvable partner are dropped here - this is the one place in the whole pipeline that drops rows on a quality check, rather than warning.

- Reads `team.silver.payment` fully-qualified. Same pipeline, cross-schema target - per Databricks' own guidance this is the correct/recommended form for a dataset outside the pipeline's default schema, and registers a normal dependency edge (confirmed via the pipeline graph: silver -> gold shows connected, same as bronze -> silver).
- **`has_partner` expectation uses `expect_or_drop`, not `expect`.** Every other quality check in this pipeline (bronze's `has_partner`, silver's `has_partner_cleaned`, `umcc_partners_quality_check`) is deliberately warn-only - nothing gets dropped anywhere else on the strength of a check. This is the one intentional exception: by the time a row reaches gold, it has already passed through three independent upstream checks confirming partner resolution, so a `Partner IS NULL` row here is either the confirmed UEL 13-14 source-data gap or a genuinely unmapped partner - either way, not usable for analysis, and not worth carrying downstream into dashboards. **Consequence: gold's row count no longer matches silver's 1:1.** Worth remembering if reconciling row counts across layers later.
- Drops `partner` (raw) - only `partner_cleaned` is carried forward, renamed to `Partner`.
- Drops `sales_paid_eur` - not needed at this layer.
- Business renames (see `RENAMES` dict): `_season_from_filename` -> `Season`, `partner_cleaned` -> `Partner`, `partner_category` -> `Partner_Category`, `invoiced_original` -> `Invoiced_date`, `due_on` -> `Invoice_due_date`, `receipt_of_payment` -> `Payment_received_date`, `sales_contracted_eur` -> `Rights_fee`. Everything else (`competition`, `invoice_number`, `invoiced_eur`, `_source_file`, `_ingested_at`) is left as-is.
- Underscores, not spaces, in the renamed columns - Delta rejects spaces in column names by default (`DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`) unless column mapping is enabled on the table. Decided against enabling column mapping; space-friendly labels are applied at the dashboard/BI layer instead, not stored as physical column names.
- Enriched with `cycle` from `team.dim.umccseasoncycle`, joined on `Season`. The join relies on Spark's default case-insensitive column resolution (`Season` on this side matching `season` on the dim side) rather than an explicit alias - works under default Spark settings, but would break if case-sensitive resolution were ever turned on for this workspace.
- `Payment_received_in_days` = `Payment_received_date` minus `Invoice_due_date`. Positive = paid late, negative = paid early.
- **`Payment_Diligence_Invoice` is a strict, no-grace-period rule** (revised from an earlier 30-day grace period): paid on or before `Invoice_due_date` = `Prompt Payer`; paid even one day after = `Slow Payer`. `PROMPT_PAYER_MAX_DAYS = 0` controls the cutoff.
- **Two independent null checks, evaluated in order, both required:**
  1. `Invoice_due_date IS NULL` -> `NULL`. No due date means no assessment is possible at all - checked first, so nothing downstream of it matters if this fires.
  2. `Payment_received_date IS NULL` -> `NULL`. Not yet paid, not "Slow Payer" by omission. This check exists deliberately: `datediff(NULL, x)` evaluates to `NULL` in SQL, and `NULL <= 0` is `NULL` (not true), so without this explicit check an unpaid invoice would silently fall through to `Slow Payer` via `.otherwise()` - the opposite of the intended behavior.
- `Payment_Diligence_Season` (per `Partner` + `Season`, majority vote): counts `Prompt Payer` vs `Slow Payer` invoices within the group, using a `Window.partitionBy("Partner", "Season")` - rows with a null `Payment_Diligence_Invoice` don't count toward either side. Whichever has more wins; a tie defaults to `Slow Payer` (conservative default, deliberate choice); a group with zero assessable invoices gets `NULL`.
- Deliberately two distinct names (`Payment_Diligence_Invoice` / `Payment_Diligence_Season`), not case-variants of the same string - Spark columns are case-preserving but not case-sensitive by default, so names differing only by case risk ambiguous-reference errors downstream.

```python
"""Gold transformation: team.gold.payment"""

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROMPT_PAYER_MAX_DAYS = 0  # paid on or before due date = Prompt Payer; any days late = Slow Payer

RENAMES = {
    "_season_from_filename": "Season",
    "partner_cleaned": "Partner",
    "partner_category": "Partner_Category",
    "invoiced_original": "Invoiced_date",
    "due_on": "Invoice_due_date",
    "receipt_of_payment": "Payment_received_date",
    "sales_contracted_eur": "Rights_fee",
}


@dp.table(
    name="team.gold.payment",
    comment=(
        "Gold: business-renamed, analysis-ready payment table with "
        "per-invoice and per-partner-season payment diligence. Rows with "
        "no resolved Partner are dropped (see has_partner expectation)."
    ),
)
@dp.expect_or_drop("has_partner", "Partner IS NOT NULL")
def payment():
    df = spark.read.table("team.silver.payment")

    df = df.drop("partner", "sales_paid_eur")

    for old, new in RENAMES.items():
        df = df.withColumnRenamed(old, new)

    # Enrich with UMCC season-cycle reference data. Single-name join on
    # "Season" relies on Spark's case-insensitive column resolution to
    # match against the dim's "season" column with no duplicate key column.
    season_cycle = spark.read.table("team.dim.umccseasoncycle").select("season", "cycle")
    df = df.join(season_cycle, "Season", "left")

    df = df.withColumn(
        "Payment_received_in_days",
        F.datediff(F.col("Payment_received_date"), F.col("Invoice_due_date")),
    )

    # Null due date -> no assessment possible, tag is null.
    # Null received date -> not yet paid, also null (not Slow by omission -
    # datediff(NULL, x) is NULL, and NULL <= 0 is NULL in SQL, not True, so
    # without this explicit check it would fall through to "Slow Payer" via
    # .otherwise(), which is wrong).
    df = df.withColumn(
        "Payment_Diligence_Invoice",
        F.when(F.col("Invoice_due_date").isNull(), F.lit(None))
        .when(F.col("Payment_received_date").isNull(), F.lit(None))
        .when(F.col("Payment_received_in_days") <= PROMPT_PAYER_MAX_DAYS, F.lit("Prompt Payer"))
        .otherwise(F.lit("Slow Payer")),
    )

    # Majority vote per (Partner, Season), counting only invoices with a
    # non-null Payment_Diligence_Invoice - rows with no payment received
    # date don't influence the count either way.
    partner_season_win = Window.partitionBy("Partner", "Season")
    prompt_count = F.sum(
        F.when(F.col("Payment_Diligence_Invoice") == "Prompt Payer", 1).otherwise(0)
    ).over(partner_season_win)
    slow_count = F.sum(
        F.when(F.col("Payment_Diligence_Invoice") == "Slow Payer", 1).otherwise(0)
    ).over(partner_season_win)

    df = df.withColumn(
        "Payment_Diligence_Season",
        F.when((prompt_count + slow_count) == 0, F.lit(None))  # no assessable invoices
        .when(prompt_count > slow_count, F.lit("Prompt Payer"))
        .otherwise(F.lit("Slow Payer")),  # covers both real Slow majority and ties
    )

    return df
```
