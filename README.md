# Medicare Inpatient Cost and Care Analysis

## Overview
This project analyzes Medicare inpatient hospital data to provide actionable insights into healthcare delivery and costs. Using PostgreSQL, it features an ETL pipeline with logging, followed by advanced SQL queries to examine costs, utilization, demographics, and chronic conditions, supporting healthcare optimization and policy decisions.

## Problem Statement
As a stakeholder, the goal is to leverage the Medicare Inpatient Hospitals dataset to gain insights into healthcare delivery and costs for Medicare beneficiaries. The specific objectives are:
- **Analyze the distribution of healthcare costs** across providers and regions (states).
- **Understand utilization patterns**, such as discharges and covered days, to assess service volume and intensity.
- **Explore demographic trends** (e.g., age, gender, race) and their impact on healthcare costs and utilization.
- **Identify high-cost areas and providers** to pinpoint inefficiencies or outliers in spending.
- **Assess the prevalence of chronic conditions** (e.g., diabetes, heart failure) to inform resource allocation and policy decisions.

This analysis aims to optimize healthcare spending, improve service delivery, and target interventions for prevalent conditions.

## ETL Process
### How ETL Was Done
The ETL process was fully implemented in PostgreSQL to handle the raw CSV data:
- **Extract**: Loaded the CSV file (encoded in `ISO-8859-1`) into a staging table using `\copy`.
- **Transform**: Cleaned data (e.g., NULLs replaced with 0) and filtered to essential columns.
- **Load**: Inserted transformed data into a final table, logged in `etl_log`.

### Staging Table Creation
The staging table, `staging_inpatient_data`, captured all 56 columns:
- **Structure**: Matched CSV schema with types like `INTEGER`, `FLOAT`, `VARCHAR`.
- **Adjustment**: Expanded `"Rndrng_Prvdr_RUCA_Desc"` to `VARCHAR(100)` for longer values.
- **Purpose**: Enabled raw data exploration before final selection.

### Final Table Selection Based on Problem Statement
The final table, `inpatient_data`, included 16 columns:
- **Provider Info**: `"Rndrng_Prvdr_CCN"`, `"Rndrng_Prvdr_Org_Name"`, `"Rndrng_Prvdr_State_Abrvtn"`.
- **Costs**: `"Tot_Submtd_Cvrd_Chrg"`, `"Tot_Pymt_Amt"`, `"Tot_Mdcr_Pymt_Amt"`.
- **Utilization**: `"Tot_Dschrgs"`, `"Tot_Cvrd_Days"`.
- **Demographics**: `"Bene_Avg_Age"`, `"Bene_Feml_Cnt"`, `"Bene_Male_Cnt"`, `"Bene_Race_Wht_Cnt"`, `"Bene_Race_Black_Cnt"`.
- **Chronic Conditions**: `"Bene_CC_PH_Diabetes_V2_Pct"`, `"Bene_CC_PH_HF_NonIHD_V2_Pct"`.
- **Risk**: `"Bene_Avg_Risk_Scre"`.

### Transformations Made
- **NULL Handling**: Used `COALESCE` to replace NULLs with 0.
- **Column Filtering**: Extracted 16 columns from 56 based on analysis goals.
- **Data Integrity**: Verified counts and sampled data.

## Achievements and Findings

This project successfully processed and analyzed Medicare inpatient hospital data to meet the outlined objectives. Below are the key achievements and insights derived from the ETL pipeline and SQL-based analysis:

### Achievements
- **Robust ETL Pipeline**: Implemented a PostgreSQL ETL process to extract data from a 56-column CSV, transform it by handling NULL values, and load 16 essential columns into an analysis-ready table, ensuring data quality and usability.
- **Process Transparency**: Established a logging system in PostgreSQL (`etl_log` table) to track ETL steps, providing a reliable audit trail and facilitating troubleshooting.
- **Comprehensive Analysis**: Developed advanced SQL queries to address cost distribution, utilization patterns, demographic trends, high-cost outliers, and chronic condition prevalence, delivering actionable insights.

### Key Findings
- **Cost Distribution**:
  - Identified states with the highest average total and Medicare payments, revealing regional cost disparities (e.g., via `AVG("Tot_Pymt_Amt")` by state).
  - Pinpointed top 10 high-cost providers, showing specific facilities driving spending, often with significant gaps between charges and payments.
- **Utilization Patterns**:
  - Uncovered states and providers with the highest discharges and covered days, indicating high service volume (e.g., `SUM("Tot_Dschrgs")`).
  - Found outliers with extreme utilization (e.g., discharges > 2 standard deviations above mean), often linked to longer average stays.
- **Demographic Trends**:
  - Revealed variations in average age, gender ratios, and racial composition across states, with older or female-dominant areas showing higher costs (e.g., `AVG("Bene_Avg_Age")`, `SUM("Bene_Feml_Cnt")`).
  - Segmented cost and utilization by age groups and gender dominance, highlighting demographic impacts on healthcare needs.
- **High-Cost Areas and Providers**:
  - Calculated cost-per-discharge and cost-per-day ratios, identifying inefficient states with high unit costs (e.g., `SUM("Tot_Pymt_Amt") / SUM("Tot_Dschrgs")`).
  - Detected outlier providers with excessive payments, often tied to higher patient risk scores (`"Bene_Avg_Risk_Scre"`), justifying some cost increases.
- **Chronic Condition Prevalence**:
  - Assessed diabetes and heart failure prevalence by state, finding regions with significant burdens (e.g., `AVG("Bene_CC_PH_Diabetes_V2_Pct")`).
  - Established correlations between chronic conditions and costs/utilization (e.g., `CORR(diabetes_pct, total_payment)`), showing diabetes as a key cost driver.

These findings provide a foundation for optimizing Medicare spending, targeting high-cost or high-prevalence areas, and tailoring healthcare services to demographic and clinical needs.

## Features
- **Cost Analysis**: Identifies high-cost states and providers.
- **Utilization Insights**: Examines discharges and covered days.
- **Demographic Trends**: Analyzes age, gender, and race impacts.
- **Chronic Condition Prevalence**: Assesses diabetes and heart failure burdens.
- **Logging**: Tracks ETL steps for transparency.
