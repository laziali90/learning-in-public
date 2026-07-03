# DIM Tables Setup

This notebook creates and populates all dimension tables in the `team.dim` schema. These tables define the allowed values for columns that require exact match validation in the quality check notebook.

**Run this notebook once to set up the DIM tables. After that, only run it again if allowed values change (e.g. a new platform is added). Use `INSERT` to add new values rather than re-running the full notebook, to avoid overwriting existing data.**

**Notebook:** `TEAM > Social data_CONF_dim tables automated setup`

---

## PLATFORM
Allowed social media platforms.

```sql
CREATE TABLE IF NOT EXISTS team.dim.social_platform (platform_value STRING);

INSERT INTO team.dim.social_platform VALUES
('Instagram'), ('Tiktok'), ('Twitter'), ('Youtube'), ('Weibo'), ('VK'), ('Facebook');

SELECT * FROM team.dim.social_platform;
```

---

## SEASON
Allowed UEFA competition season identifiers.

```sql
CREATE TABLE IF NOT EXISTS team.dim.umcc_season (season_value STRING);

INSERT INTO team.dim.umcc_season VALUES
('UCL'), ('UEL'), ('UECL');

SELECT * FROM team.dim.umcc_season;
```

---

## STAGE
Allowed competition stages.

```sql
CREATE TABLE IF NOT EXISTS team.dim.umcc_stage (stage_value STRING);

INSERT INTO team.dim.umcc_stage VALUES
('GS'), ('KO'), ('LP');

SELECT * FROM team.dim.umcc_stage;
```

---

## MONTH
Allowed month names. Season runs May through April.

```sql
CREATE TABLE IF NOT EXISTS team.dim.month (month_value STRING);

INSERT INTO team.dim.month VALUES
('May'), ('June'), ('July'), ('August'), ('September'), ('October'),
('November'), ('December'), ('January'), ('February'), ('March'), ('April');

SELECT * FROM team.dim.month;
```

---

## GS_KO
Allowed values for the GS/KO column (cleaned from bronze).

```sql
CREATE TABLE IF NOT EXISTS team.dim.umcc_gs_ko (gs_ko_value STRING);

INSERT INTO team.dim.umcc_gs_ko VALUES
('GS'), ('KO'), ('LP');

SELECT * FROM team.dim.umcc_gs_ko;
```

---

## COVID_DATES
Allowed COVID period classifications.

```sql
CREATE TABLE IF NOT EXISTS team.dim.umcc_covid_dates (covid_dates_value STRING);

INSERT INTO team.dim.umcc_covid_dates VALUES
('Pre COVID'), ('COVID'), ('Post COVID'), ('NOT MATCHWEEK');

SELECT * FROM team.dim.umcc_covid_dates;
```

---

## GROUPTYPE
Allowed group type classifications.

```sql
CREATE TABLE IF NOT EXISTS team.dim.grouptype (grouptype_value STRING);

INSERT INTO team.dim.grouptype VALUES
('Owned'), ('UnOwned');

SELECT * FROM team.dim.grouptype;
```

---

## LOGOPRESENT
Allowed values for logo presence flag. `0` = not present, `1` = present.

```sql
CREATE TABLE IF NOT EXISTS team.dim.logopresent (logopresent_value INTEGER);

INSERT INTO team.dim.logopresent VALUES
(0), (1);

SELECT * FROM team.dim.logopresent;
```

---

## TEXTPRESENT
Allowed values for text presence flag. `0` = not present, `1` = present.

```sql
CREATE TABLE IF NOT EXISTS team.dim.textpresent (textpresent_value INTEGER);

INSERT INTO team.dim.textpresent VALUES
(0), (1);

SELECT * FROM team.dim.textpresent;
```

---

## WEEKDAY
Allowed weekday names.

```sql
CREATE TABLE IF NOT EXISTS team.dim.weekday (weekday_value STRING);

INSERT INTO team.dim.weekday VALUES
('Monday'), ('Tuesday'), ('Wednesday'), ('Thursday'), ('Friday'), ('Saturday'), ('Sunday');

SELECT * FROM team.dim.weekday;
```

---

## Verify All DIM Tables

```sql
SHOW TABLES IN team.dim;
```
