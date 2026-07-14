# Enhanced Social Dashboard

## Short description

This dashboard provides enhanced social media analytics for UMCC, additionally to existing data deliverables by the Vendor datasets. It is designed to:

- Track organic reach and engagement performance over time, particularly on an account level
- Detect algorithmic changes through rolling trend analysis
- Identify anomalous posts (viral, underperforming, or hidden gems) and its amount, which further determines the
- Measure content virality on a platform-specific basis

> **Disclaimer:** This document has been anonymized. All references to the originating organization, its internal systems, and its data vendor have been replaced with generic placeholders. It is shared purely to illustrate the data pipeline logic, transformation steps, and DAX/Power Query methodology — not to disclose any proprietary or organization-specific information.

**Data sources:** [Microsoft SharePoint](https://company.sharepoint.com/:f:/r/sites/CompanyInsights/Shared%20Documents/General/UCC%20Social/UMCC%20Social%20database?csf=1&web=1&e=XXXXXX)

**Data access / BI visualization:** [Microsoft Power BI Desktop](https://app.powerbi.com/groups/me/reports/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/xxxxxxxxxxxxxxxx?experience=power-bi)

**First created:** May 2026

**Created by / Responsible:** Data Analyst

**Status:** 🟢 Active

---

## Data Ingestion and Transformation
---

## Data Ingestion and Transformation

### Raw Data ingestion (BRONZE)

1) Data manually downloaded from Vendor-provided dashboards (login required) - tab "Campaign posts"

`https://bi-eu.vendorbi.com/#/site/UEFAClubCompetitions/views/UEFACampaignReporting-UCL/CampaignSummary?:iid=1`
`https://bi-eu.vendorbi.com/t/UEFAClubCompetitions/views/UELUECLCampaignReporting/CampaignPosts?%3Aembed=y&%3AisGuestRedirectFromVizportal=y`

2) Data stored on Company Insights SharePoint

`https://company.sharepoint.com/:f:/r/sites/CompanyInsights/Shared%20Documents/General/UCC%20Social/UMCC%20Social%20database?csf=1&web=1&e=XXXXXX`

3) Data connected via built in connector for SharePoint in Power BI

<img width="482" height="282" alt="image" src="https://github.com/user-attachments/assets/24adde49-6851-4c0e-976f-b1ff5aa157bb" />

---

### Data transformation (SILVER)

The following transformations are all executed via Microsoft Power Query - split by the datasets:

#### L DIM Date transform
A date dimension table covering 01/01/2018 to 31/12/2030 with German long-form dates used for joining to source data. Required to clean and correct incorrect date provided by original tables. What does this code do:
- Connects to the Company Insights SharePoint site and loads the DIM_Dates transform.xlsx file from the UMCC Social database folder
- Promotes the first row as column headers
- Sets German Date as text format and Date corrected as date format

| Column | Format | Example |
|---|---|---|
| German Date | `D. Month YYYY` | `1. Januar 2018` |
| Date Corrected | `DD/MM/YYYY` | `01/01/2018` |


```powerquery
let
    Source = SharePoint.Files("https://company.sharepoint.com/sites/CompanyInsights", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Name], "DIM")),
    #"DIM_Dates transform xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/" = #"Filtered Rows"{[Name="DIM_Dates transform.xlsx",#"Folder Path"="https://company.sharepoint.com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"DIM_Dates transform xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"),
    DIM_Date_Sheet = #"Imported Excel Workbook"{[Item="DIM_Date",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(DIM_Date_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"German Date", type text}, {"Date corrected", type date}})
in
    #"Changed Type"
```
#### LT UCL Social data ingest

Table containing all UCL data from the Vendor extract. What does this code do:
- Connects to the Company Insights SharePoint site and loads the 2122_2526 Partner Campaign Posts UCL.xlsx file from the UMCC Social database folder
- Removes unnecessary metadata columns (Date accessed, Date modified, Attributes)
- Promotes the first row as column headers
- Removes columns not required for analysis, retaining only core performance and media value metrics
- Sets correct data types: MSI, VIDEOVIEWS, INTERACTIONS_VIEWS, IMPRESSIONS_VIEWS, REACH as whole numbers, ENGAGEMENTRATE and REACHRATE as percentages, and LEVEL_4_MEDIA_VALUE as a decimal number

```powerquery
let
    Source = SharePoint.Files("https://company.sharepoint.com/sites/CompanyInsights", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "UMCC Social database")),
    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each Text.Contains([Extension], ".xlsx")),
    #"Filtered Rows2" = Table.SelectRows(#"Filtered Rows1", each Text.Contains([Name], "UCL")),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows2",{"Date accessed", "Date modified", "Attributes"}),
    #"2122_2526 Partner Campaign Posts UCL xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/" = #"Removed Columns"{[Name="2122_2526 Partner Campaign Posts UCL.xlsx",#"Folder Path"="https://company.sharepoint.com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"2122_2526 Partner Campaign Posts UCL xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"),
    #"Sheet 1_Sheet" = #"Imported Excel Workbook"{[Item="Sheet 1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Sheet 1_Sheet", [PromoteAllScalars=true]),
    #"Removed Columns1" = Table.RemoveColumns(#"Promoted Headers",{"Post Description", "Day of Date", "Minute of Time Posted", "Ambassador Post", "Collab Post", "MPRO Campaign", "LIKES", "COMMENTS", "SHARES", "TOTALVALUE", "PROMOTIONQUALITY", "TEXTPRESENT", "LOGOPRESENT", "VPL", "VPC", "VPS", "VPV", "MAX_VALUE_PEAK_INT_VAL_EUROS", "ADVALUE_LIKES", "ADVALUE_COMMENTS", "ADVALUE_SHARES", "ADVALUE_VIEWS", "VPC_ADJ", "VPL_ADJ", "VPS_ADJ", "MAX_VALUE_PEAK_INT_VALUE_ADJ_EUROS", "LEVEL_1_MEDIA_VALUE", "LEVEL_2_MEDIA_VALUE", "LEVEL_3_MEDIA_VALUE", "LEVEL_5_MEDIA_VALUE", "LEVEL_6_MEDIA_VALUE", "#", "Round", "Matchday", "VPV_ADJ"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns1",{{"MSI", Int64.Type}, {"VIDEOVIEWS", Int64.Type}, {"INTERACTIONS_VIEWS", Int64.Type}, {"IMPRESSIONS_VIEWS", Int64.Type}, {"REACH", Int64.Type}, {"ENGAGEMENTRATE", Percentage.Type}, {"REACHRATE", Percentage.Type}, {"LEVEL_4_MEDIA_VALUE", type number}})
in
    #"Changed Type"

```

#### LT UELUECL Social data ingest

Table containing all UELUECL data from the Vendor extract. What does this code do:
- Connects to the Company Insights SharePoint site and loads the 2122_2526 Partner Campaign Posts UELUECL.xlsx file from the UMCC Social database folder
- Removes unnecessary metadata columns (Date accessed, Date modified, Attributes)
- Promotes the first row as column headers
- Renames German column headers to English: "Minute von Time Posted" → "Minute of Time Posted", "Tag von Week W/C" → "Day of Week W/C" and "VPV_" → "VPV"
- Removes columns not required for analysis, retaining only core performance and media value metrics
- Sets correct data types: MSI, VIDEOVIEWS, INTERACTIONS_VIEWS, IMPRESSIONS_VIEWS, REACH as whole numbers, ENGAGEMENTRATE and REACHRATE as percentages, and LEVEL_4_MEDIA_VALUE as a decimal number

```powerquery
let
    Source = SharePoint.Files("https://company.sharepoint.com/sites/CompanyInsights", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "UMCC Social database")),
    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each Text.Contains([Extension], ".xlsx")),
    #"Filtered Rows2" = Table.SelectRows(#"Filtered Rows1", each Text.Contains([Name], "UELUECL")),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows2",{"Date accessed", "Date modified", "Attributes"}),
    #"2122_2526 Partner Campaign Posts UCL xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/" = #"Removed Columns"{[Name="2122_2526 Partner Campaign Posts UELUECL.xlsx",#"Folder Path"="https://company.sharepoint.com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"2122_2526 Partner Campaign Posts UCL xlsx_https://company sharepoint com/sites/CompanyInsights/Shared Documents/General/UCC Social/UMCC Social database/"),
    #"Sheet 1_Sheet" = #"Imported Excel Workbook"{[Item="Sheet 1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Sheet 1_Sheet", [PromoteAllScalars=true]),
    #"Renamed Columns" = Table.RenameColumns(#"Promoted Headers",{{"Minute von Time Posted", "Minute of Time Posted"}, {"VPV_", "VPV"}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Renamed Columns",{"Post Description", "Day of Date", "Minute of Time Posted", "Ambassador Post", "Collab Post", "MPRO Campaign", "LIKES", "COMMENTS", "SHARES", "TOTALVALUE", "PROMOTIONQUALITY", "TEXTPRESENT", "LOGOPRESENT", "VPL", "VPC", "VPS", "VPV", "MAX_VALUE_PEAK_INT_VAL_EUROS", "ADVALUE_LIKES", "ADVALUE_COMMENTS", "ADVALUE_SHARES", "ADVALUE_VIEWS", "VPC_ADJ", "VPL_ADJ", "VPS_ADJ", "MAX_VALUE_PEAK_INT_VALUE_ADJ_EUROS", "LEVEL_1_MEDIA_VALUE", "LEVEL_2_MEDIA_VALUE", "LEVEL_3_MEDIA_VALUE", "LEVEL_5_MEDIA_VALUE", "LEVEL_6_MEDIA_VALUE", "#", "Round", "Matchday", "VPV_ADJ"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns1",{{"Tag von Week W/C", "Day of Week W/C"}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Renamed Columns1",{{"MSI", Int64.Type}, {"VIDEOVIEWS", Int64.Type}, {"INTERACTIONS_VIEWS", Int64.Type}, {"IMPRESSIONS_VIEWS", Int64.Type}, {"REACH", Int64.Type}, {"ENGAGEMENTRATE", Percentage.Type}, {"REACHRATE", Percentage.Type}, {"LEVEL_4_MEDIA_VALUE", type number}})
in
    #"Changed Type"
```

#### APP Data

Combined dataset containing all data from LT UELUECL Social data ingest and LT UCL Social data ingest. What does this code do:
- Combines the UCL and UELUECL data into a single table
- Joins to the DIM Date table on the German date column to add the Date corrected field
- Sets Date corrected as a date format
- Replaces all 0 values in ENGAGEMENTRATE and REACH with null to avoid skewing aggregations

```powerquery
let
    Source = Table.Combine({#"LT UCL Social data ingest", #"LT UELUECL Social data ingest"}),
    #"Merged Queries" = Table.NestedJoin(Source, {"Day of Week W/C"}, #"L DIM Date transform", {"German Date"}, "L DIM Date transform", JoinKind.LeftOuter),
    #"Expanded L DIM Date transform" = Table.ExpandTableColumn(#"Merged Queries", "L DIM Date transform", {"Date corrected"}, {"Date corrected"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Expanded L DIM Date transform",{{"Date corrected", type date}}),
    #"Replace Zeros ER" = Table.ReplaceValue(#"Changed Type", 0, null, Replacer.ReplaceValue, {"ENGAGEMENTRATE"}),
    #"Replace Zeros Reach" = Table.ReplaceValue(#"Replace Zeros ER", 0, null, Replacer.ReplaceValue, {"REACH"})
in
    #"Replace Zeros Reach"

```

---

### Data enrichment (Gold)

The following are bespoke metrics, created to further analyze respective social media data and cover the knowledge gaps accordingly.


#### Reach_to_Impressions_Ratio
The proportion of total reach relative to total impressions. A value close to 1 means content is being shown to a wide variety of unique users. A declining ratio over time indicates the algorithm is repeatedly serving content to the same audience — an early warning sign of reduced organic distribution.

```dax
Reach_to_Impressions_Ratio = 
DIVIDE(SUM('APP Data'[REACH]), SUM('APP Data'[IMPRESSIONS_VIEWS]))
```

#### Median_Reach
The middle value of reach across all posts in a given period. Unlike the average, the median is not distorted by viral outliers, making it the most reliable representation of typical post reach.

```dax
Median_Reach = MEDIAN('APP Data'[REACH])
```

#### Reach_Rolling_90d
The median reach across all posts published in the 90 days preceding any given date. Captures structural shifts in organic distribution — sudden sustained drops are strong indicators of an algorithm change.

```dax
Reach_Rolling_90d = 
CALCULATE(
    MEDIAN('APP Data'[REACH]),
    DATESINPERIOD('APP Data'[Date Corrected], MAX('APP Data'[Date Corrected]), -90, DAY))
```

#### Reach_Rolling_90d_YT
The median reach across all posts published in the 90 days preceding any given date, based on impressions & views because for YouTube, no reach figures are available. In essence the same as Reach_Rolling_90d.

```dax
CALCULATE(
    MEDIAN('APP Data'[IMPRESSIONS_VIEWS]),
    DATESINPERIOD('APP Data'[Date corrected], MAX('APP Data'[Date corrected]), -90, DAY))
```

#### Reach_25th_Percentile
The reach value below which 25% of all posts fall. Represents the lower boundary of typical post performance.

```dax
Reach_25th_Percentile = PERCENTILE.INC('APP Data'[REACH], 0.25)
```

#### Reach_75th_Percentile
The reach value below which 75% of all posts fall. Represents the upper boundary of typical post performance. The band between the 25th and 75th percentile shows where 50% of posts land — a wide band indicates high volatility, a narrow band indicates consistency.

```dax
Reach_75th_Percentile = PERCENTILE.INC('APP Data'[REACH], 0.75)
```

---

#### Median_MSI
The middle value of Meaningful Social Interactions (MSI) across all posts in a given period.

```dax
Median_MSI = MEDIAN('APP Data'[MSI])
```

#### Engagement_Rolling_90d
The median engagement rate across all posts published in the 90 days preceding any given date. Smooths out day-to-day fluctuations to reveal the underlying trend in content resonance. A sustained drop signals either an algorithm change or declining content relevance.

```dax
Engagement_Rolling_90d = 
CALCULATE(
    MEDIAN('APP Data'[ENGAGEMENTRATE]),
    DATESINPERIOD('APP Data'[Date corrected], MAX('APP Data'[Date corrected]), -90, DAY))
```

#### Engagement_Rolling_90d_TK_YT
The median engagement rate across all posts published in the 90 days preceding any given date. Same as Engagement_Rolling_90d but to be used for TikTok and YouTube because no engagement rate available.

```dax
CALCULATE(
    MEDIAN('APP Data'[MSI]),
    DATESINPERIOD('APP Data'[Date corrected], MAX('APP Data'[Date corrected]), -90, DAY)
)
```

---


#### Virality_Cluster 2

This calculation classifies every post into a performance cluster by evaluating two independent dimensions: how much media value it generated and how widely it was distributed — both measured relative to comparable posts only, not the entire dataset.

Rather than benchmarking every post against the global average, each post is compared exclusively against posts from the same competition, platform and post type published in the 3 months prior to that post's publish date. This makes the benchmark dynamic — it reflects the current algorithm environment and content mix rather than historical norms that are no longer relevant.

The virality cluster consists of 2 scores, which are explained further below. Note: In Power BI / DAX, the measure is saved as one code block. It is split below.

**Media Value Score (VS)**

The first dimension measures whether the post generated more or less media value than a typical peer post. A score of +200 means the post generated three times the expected media value for its context — a strong signal of content quality and commercial impact.


```dax
VAR PostDate = 'APP Data'[Post_Date_Key]
VAR StartDate = EDATE(PostDate, -3)
VAR PostPlatform = 'APP Data'[Platform]
VAR PostType = 'APP Data'[Post Type]
VAR PostCompetition = 'APP Data'[Competition]
VAR CurrentValue = 'APP Data'[LEVEL_4_MEDIA_VALUE]
VAR PostReachRate = 'APP Data'[REACHRATE]

VAR ExpectedValue =
    CALCULATE(
        MEDIAN('APP Data'[LEVEL_4_MEDIA_VALUE]),
        FILTER(
            ALL('APP Data'),
            'APP Data'[Platform] = PostPlatform &&
            'APP Data'[Post Type] = PostType &&
            'APP Data'[Competition] = PostCompetition &&
            'APP Data'[Date corrected] >= StartDate &&
            'APP Data'[Date corrected] <= PostDate
        )
    )

VAR VS =
    IF(
        ISBLANK(ExpectedValue),
        0,
        DIVIDE(CurrentValue - ExpectedValue, ExpectedValue) * 100
    )
```


**Reach Rate Z-Score (RZ)**

The second dimension measures how far the post's reach rate deviated from the average of its peers, expressed in standard deviations. This normalises for audience size differences across competitions and platforms, making Instagram and Twitter posts directly comparable on distribution performance.

```dax
VAR AvgReachRate =
    CALCULATE(
        AVERAGE('APP Data'[REACHRATE]),
        FILTER(
            ALL('APP Data'),
            'APP Data'[Platform] = PostPlatform &&
            'APP Data'[Post Type] = PostType &&
            'APP Data'[Competition] = PostCompetition &&
            'APP Data'[Date corrected] >= StartDate &&
            'APP Data'[Date corrected] <= PostDate
        )
    )

VAR StDevReachRate =
    CALCULATE(
        STDEV.P('APP Data'[REACHRATE]),
        FILTER(
            ALL('APP Data'),
            'APP Data'[Platform] = PostPlatform &&
            'APP Data'[Post Type] = PostType &&
            'APP Data'[Competition] = PostCompetition &&
            'APP Data'[Date corrected] >= StartDate &&
            'APP Data'[Date corrected] <= PostDate
        )
    )

VAR RZ =
    IF(
        ISBLANK(StDevReachRate),
        0,
        DIVIDE(PostReachRate - AvgReachRate, StDevReachRate)
    )

RETURN
SWITCH(
    TRUE(),
    VS > 200 && RZ > 1,             "Viral",
    VS > 100 && RZ <= 0,            "Hidden Gem",
    VS < -50 && RZ < -1,            "Underperformer",
    VS > 100 && RZ > 0 && RZ <= 1,  "High Reach Low Engagement",
    "Normal"
)
```

Quadrant definitions:

| Flag | Description |
|---|---|
| Viral | High reach AND high engagement — algorithm amplified strong content |
| High Reach Low Engagement | Algorithm distributed widely but audience didn't respond |
| Hidden Gem | Low reach but high engagement — algorithm under-distributed quality content |
| Underperformer | Low reach AND low engagement |
