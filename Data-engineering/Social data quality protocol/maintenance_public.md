# Maintenance Scripts

> **Warning:** All scripts below are destructive and irreversible. Dropped tables cannot be recovered. Always verify you are running these in the correct environment before executing.

---

## Social data_CLEAN_Delete Bronze Tables for Reupload

Notebook: team > `Social data_CLEAN_Delete Bronze Tables for Reupload`

Use this when you need to delete and re-upload specific bronze tables (e.g. to fix column types or data errors in the source file).

```sql
DROP TABLE IF EXISTS team.landing_bronze.2526_ucl_audience_byday;
DROP TABLE IF EXISTS team.landing_bronze.2526_ucl_audience_byposttype;
DROP TABLE IF EXISTS team.landing_bronze.2526_ucl_valuation_memberposts;
DROP TABLE IF EXISTS team.landing_bronze.2526_ucl_valuation_uniquememberposts;
```

---

## Social data_CLEAN_Delete Silver and Gold Tables for Reupload

Notebook: team> `Social data_CLEAN_Delete Silver and Gold Tables for Reupload`

Use this when you need to do a full reset of silver and gold — for example when column types in bronze were wrong and silver/gold were built from incorrect data. After running this, fix the source files, re-upload to bronze and re-run the full pipeline job.

```sql
-- Drop gold tables
DROP TABLE IF EXISTS team.gold.audience_byposttype;
DROP TABLE IF EXISTS team.gold.audience_byposttype_quarantine;
DROP TABLE IF EXISTS team.gold.audience_byday;
DROP TABLE IF EXISTS team.gold.audience_byday_quarantine;
DROP TABLE IF EXISTS team.gold.valuation_memberposts;
DROP TABLE IF EXISTS team.gold.valuation_memberposts_quarantine;
DROP TABLE IF EXISTS team.gold.valuation_uniquememberposts;
DROP TABLE IF EXISTS team.gold.valuation_uniquememberposts_quarantine;

-- Drop silver tables
DROP TABLE IF EXISTS team.silver.2526_ucl_audience_byday;
DROP TABLE IF EXISTS team.silver.2526_ucl_audience_byposttype;
DROP TABLE IF EXISTS team.silver.2526_ucl_valuation_memberposts;
DROP TABLE IF EXISTS team.silver.2526_ucl_valuation_uniquememberposts;
DROP TABLE IF EXISTS team.silver.2526_uecl_audience_byday;
DROP TABLE IF EXISTS team.silver.2526_uecl_audience_byposttype;
DROP TABLE IF EXISTS team.silver.2526_uel_audience_byday;
DROP TABLE IF EXISTS team.silver.2526_uel_audience_byposttype;
DROP TABLE IF EXISTS team.silver.2526_ueluecl_valuation_memberposts;
```
