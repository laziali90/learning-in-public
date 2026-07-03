## Situation / Issue

In the UMCC Research programme, these datasets play a crucial role in the full season reporting workstream, are substantial in their size (in Excel terms contain 100k's of individual posts and around 40 - 70 columns) and are intended to be used for data analysis and pivoting directly out of the provided excel. Because of the short timeframe between the data delivery and reporting deadlines for UMCC Marketing partners reporting, TEAM Insights responsible was forced to completely trust the data supplier (Kore) to provide a correct dataset.

Identified datasets:

- Audiences (UEFA owned and Third party engagement monitoring to understand "buzz" around UMCCs)
  - UCL
  - UEL
  - UECL
- Valuation (UEFA owned and Third parties post monitoring, focus on post by post media valuation of all branded partners)
  - UCL
  - UEL/UECL

---

## Solution / Goal

This data pipeline is built to:
- Automatically check all provided posts, their attributes, tags as well as numerical and calculated metrics of their correctness (flagging illogical values) and format
- Compile a ready to share list consisting of all incorrect datapoints to be sent back to data supplier to be corrected, including what columns are wrong
- Provide the data analyst with a cleaned, ready for analysis dataset excluding all incorrect datasets
- All the above executed in a scalable pipeline, meaning that any similar dataset in the future, once uploaded in the defined landing zone ideally via RestAPI / datalake connector, will be automatically quality checked, errors documented and business intelligence-ready dataset without the need for manual checks

**First created:** June 2026

**Created by / Responsible:** [RVO](https://github.com/laziali90)

**Status:** 🟢 Active

---

## Pipeline Overview

The pipeline runs across three layers in a medallion architecture:

```
Bronze (raw upload)
    ↓
    Notebook 1: Rules mapping & transfer to silver
    - Table discovery and ruleset matching
    - Column name cleaning
    - Excel error cleaning
    - DIM column lowercasing
    - Audience tables: full overwrite
    - Valuation tables: upsert CDC via SOCIALPOST_ID
    ↓
Silver (cleaned data)
    ↓
    Notebook 2: Data quality checks & transfer to gold
    - DIM table lookups
    - Null, range, pattern and sum checks per column
    - Rows split into clean vs quarantine
    - Competitions unioned into combined gold tables
    ↓
Gold (analysis-ready data)
    - 4 clean tables ready for Power BI
    - 4 quarantine tables with failure reasons per row
```

All notebooks run as a single Databricks Job in sequence:  `UMCC Social data quality protocol full pipeline` . The pipeline is designed to be dataset-agnostic — adding a new dataset only requires adding one entry to the rules registry in Notebook 1 and the relevant column rules in Notebook 2.

<img width="920" height="416" alt="image" src="https://github.com/user-attachments/assets/e47f4ae6-a5ba-41e5-b86b-aa88ae6cf0b7" />


---

## Potential Improvements for the Future

#### Databricks Ingestion & All Data Storage on TEAM Owned Cloud Environment (Microsoft Azure Postgres SQL Server or similar)
For simplicity and speed, the original datasets as well as the quality protocol output has been stored on Default Databricks Cloud storage. In order for TEAM to own the data, a TEAM owned environment would be required (Microsoft Azure or similar) and to overwrite the current default / serverless cloud storage account from Databricks Free edition.

<img width="535" height="376" alt="image" src="https://github.com/user-attachments/assets/4409ec3a-a7ed-4933-9361-57e387e72c63" />

#### Data Further Processed and Connected to Power BI
To actually make this a fully automated data pipeline, the data should be analyzed in a BI solution like Power BI. This would actually allow the data to always be up to date and would reflect a fully automated, proper end to end data pipeline for highest possible efficiency.

#### Shortening of Notebooks
Reflecting, the 2 notebooks became quite long and include multiple steps. From an orchestration point and when replicating the same pipeline to other datasets and adjusting for the future. For example the transformations bronze → silver in one notebook, where the data transformations bronze → silver is a separate notebook etc. This would also allow to find any code errors when running all notebooks as one job, as all the steps would be dependent.

#### Lower Casing of All Strings for Data Checks
While lower casing is essential for DIM lookup checks, in the final BI usage, the display of such columns and entries won't look "aesthetically pleasing" whenever connected to a BI solution. One example are the attributes of column `PLATFORM` → Tiktok becomes tiktok etc. Not relevant for now but to consider whenever a similar pipeline with BI connection will be built.

#### Storage Tag for Compliance
Databricks allows to tag columns so that certain data transformations don't need to be coded / configured manually. Data handled by TEAM currently wouldn't be considered worth being tagged (like GDPR personal information etc.), but definitely a very efficient data cleaning logic if applied consistently in TEAM's future.

<img width="660" height="294" alt="image" src="https://github.com/user-attachments/assets/467d0116-1d26-4797-be62-1fee56065ea1" />

#### CSV Instead of XLS
Databricks can better automatically read and assess the correct data type of CSV than XLS — so whenever it has to be a manual upload, CSV is the preferred format.
