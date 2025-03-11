drop table if exists staging_inpatient_data;

-- Creating a staging table that holds all the columns and respective data

CREATE TABLE staging_inpatient_data (
    "Rndrng_Prvdr_CCN" INTEGER,
    "Rndrng_Prvdr_Org_Name" VARCHAR(100),
    "Rndrng_Prvdr_St" VARCHAR(100),
    "Rndrng_Prvdr_City" VARCHAR(50),
    "Rndrng_Prvdr_Zip5" INTEGER,
    "Rndrng_Prvdr_State_Abrvtn" VARCHAR(2),
    "Rndrng_Prvdr_State_FIPS" INTEGER,
    "Rndrng_Prvdr_RUCA" FLOAT,
    "Rndrng_Prvdr_RUCA_Desc" VARCHAR(100),
    "Tot_Benes" FLOAT,
    "Tot_Submtd_Cvrd_Chrg" FLOAT,
    "Tot_Pymt_Amt" FLOAT,
    "Tot_Mdcr_Pymt_Amt" FLOAT,
    "Tot_Dschrgs" INTEGER,
    "Tot_Cvrd_Days" FLOAT,
    "Tot_Days" FLOAT,
    "Bene_Avg_Age" FLOAT,
    "Bene_Age_LT_65_Cnt" FLOAT,
    "Bene_Age_65_74_Cnt" FLOAT,
    "Bene_Age_75_84_Cnt" FLOAT,
    "Bene_Age_GT_84_Cnt" FLOAT,
    "Bene_Feml_Cnt" FLOAT,
    "Bene_Male_Cnt" FLOAT,
    "Bene_Race_Wht_Cnt" FLOAT,
    "Bene_Race_Black_Cnt" FLOAT,
    "Bene_Race_API_Cnt" FLOAT,
    "Bene_Race_Hspnc_Cnt" FLOAT,
    "Bene_Race_NatInd_Cnt" FLOAT,
    "Bene_Race_Othr_Cnt" FLOAT,
    "Bene_Dual_Cnt" FLOAT,
    "Bene_Ndual_Cnt" FLOAT,
    "Bene_CC_BH_ADHD_OthCD_V1_Pct" FLOAT,
    "Bene_CC_BH_Alcohol_Drug_V1_Pct" FLOAT,
    "Bene_CC_BH_Tobacco_V1_Pct" FLOAT,
    "Bene_CC_BH_Alz_NonAlzdem_V2_Pct" FLOAT,
    "Bene_CC_BH_Anxiety_V1_Pct" FLOAT,
    "Bene_CC_BH_Bipolar_V1_Pct" FLOAT,
    "Bene_CC_BH_Mood_V2_Pct" FLOAT,
    "Bene_CC_BH_Depress_V1_Pct" FLOAT,
    "Bene_CC_BH_PD_V1_Pct" FLOAT,
    "Bene_CC_BH_PTSD_V1_Pct" FLOAT,
    "Bene_CC_BH_Schizo_OthPsy_V1_Pct" FLOAT,
    "Bene_CC_PH_Asthma_V2_Pct" FLOAT,
    "Bene_CC_PH_Afib_V2_Pct" FLOAT,
    "Bene_CC_PH_Cancer6_V2_Pct" FLOAT,
    "Bene_CC_PH_CKD_V2_Pct" FLOAT,
    "Bene_CC_PH_COPD_V2_Pct" FLOAT,
    "Bene_CC_PH_Diabetes_V2_Pct" FLOAT,
    "Bene_CC_PH_HF_NonIHD_V2_Pct" FLOAT,
    "Bene_CC_PH_Hyperlipidemia_V2_Pct" FLOAT,
    "Bene_CC_PH_Hypertension_V2_Pct" FLOAT,
    "Bene_CC_PH_IschemicHeart_V2_Pct" FLOAT,
    "Bene_CC_PH_Osteoporosis_V2_Pct" FLOAT,
    "Bene_CC_PH_Parkinson_V2_Pct" FLOAT,
    "Bene_CC_PH_Arthritis_V2_Pct" FLOAT,
    "Bene_CC_PH_Stroke_TIA_V2_Pct" FLOAT,
    "Bene_Avg_Risk_Scre" FLOAT
);

-- Now , I will create a final table consisting of all the columns that we need to address
-- our problem statement and achieve the defined objectives
drop table if exists inpatient_data;

CREATE TABLE inpatient_data (
    "Rndrng_Prvdr_CCN" INTEGER,
    "Rndrng_Prvdr_Org_Name" VARCHAR(100),
    "Rndrng_Prvdr_State_Abrvtn" VARCHAR(2),
    "Tot_Submtd_Cvrd_Chrg" FLOAT,
    "Tot_Pymt_Amt" FLOAT,
    "Tot_Mdcr_Pymt_Amt" FLOAT,
    "Tot_Dschrgs" INTEGER,
    "Tot_Cvrd_Days" FLOAT,
    "Bene_Avg_Age" FLOAT,
    "Bene_Feml_Cnt" FLOAT,
    "Bene_Male_Cnt" FLOAT,
    "Bene_Race_Wht_Cnt" FLOAT,
    "Bene_Race_Black_Cnt" FLOAT,
    "Bene_CC_PH_Diabetes_V2_Pct" FLOAT,
    "Bene_CC_PH_HF_NonIHD_V2_Pct" FLOAT,
    "Bene_Avg_Risk_Scre" FLOAT
);

-- Lets create a log file to record all transactions and the database modification made by each transaction

create table etl_log
(
id serial primary key,
	event_time timestamp default current_timestamp,
	message text
)

-- Now , Lets load the entire dataset in the staging csv for future usecases

copy staging_inpatient_data
from 'C:\Users\PREODATOR HELIOS 300\Desktop\project_sql\ETL project\2022\Medicare Inpatient Hospital.csv'
with (format csv,header,encoding 'ISO-8859-1');

-- Log the load

Insert into
etl_log (message) values ('CSV loaded into staging_inpatient_data with adjusted Rndrng_Prvdr_RUCA_Desc length');

-- Transform and load into the final table with only essential columns
INSERT INTO inpatient_data (
    "Rndrng_Prvdr_CCN",
    "Rndrng_Prvdr_Org_Name",
    "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Submtd_Cvrd_Chrg",
    "Tot_Pymt_Amt",
    "Tot_Mdcr_Pymt_Amt",
    "Tot_Dschrgs",
    "Tot_Cvrd_Days",
    "Bene_Avg_Age",
    "Bene_Feml_Cnt",
    "Bene_Male_Cnt",
    "Bene_Race_Wht_Cnt",
    "Bene_Race_Black_Cnt",
    "Bene_CC_PH_Diabetes_V2_Pct",
    "Bene_CC_PH_HF_NonIHD_V2_Pct",
    "Bene_Avg_Risk_Scre"
)
SELECT
    "Rndrng_Prvdr_CCN",
    "Rndrng_Prvdr_Org_Name",
    "Rndrng_Prvdr_State_Abrvtn",
    COALESCE("Tot_Submtd_Cvrd_Chrg", 0),
    COALESCE("Tot_Pymt_Amt", 0),
    COALESCE("Tot_Mdcr_Pymt_Amt", 0),
    COALESCE("Tot_Dschrgs", 0),
    COALESCE("Tot_Cvrd_Days", 0),
    COALESCE("Bene_Avg_Age", 0),
    COALESCE("Bene_Feml_Cnt", 0),
    COALESCE("Bene_Male_Cnt", 0),
    COALESCE("Bene_Race_Wht_Cnt", 0),
    COALESCE("Bene_Race_Black_Cnt", 0),
    COALESCE("Bene_CC_PH_Diabetes_V2_Pct", 0),
    COALESCE("Bene_CC_PH_HF_NonIHD_V2_Pct", 0),
    COALESCE("Bene_Avg_Risk_Scre", 0)
FROM staging_inpatient_data;

-- Log final load
Insert into etl_log (message) values ('Essential columns loaded into inpatient_data');

-- Now we will view the final loaded data

select * from inpatient_data
Limit 5;

-- Lets view the log as well
SELECT * FROM etl_log ORDER BY event_time;

-- Analyze cost distribution across state

-- Obj : To find the average total payment, Medicare payment , and submitted charges per state,
-- along with the number of unique providers in each state

select "Rndrng_Prvdr_State_Abrvtn" as states,
avg("Tot_Pymt_Amt") as avg_total_payment,
avg("Tot_Mdcr_Pymt_Amt") as avg_medicare_payment,
avg("Tot_Submtd_Cvrd_Chrg") as avg_charges,
count(Distinct ("Rndrng_Prvdr_CCN")) as provider_count
from
inpatient_data
group by "Rndrng_Prvdr_State_Abrvtn"
order by avg_total_payment desc;

-- Top 10 High Cost provider

-- Objective : To identify 10 providers with the highest total payments, including their Medicare
-- payments and charges

select "Rndrng_Prvdr_Org_Name" as provider_name,
"Rndrng_Prvdr_State_Abrvtn" as states,
"Tot_Pymt_Amt" as total_payment,
"Tot_Mdcr_Pymt_Amt" as medicare_payment,
"Tot_Submtd_Cvrd_Chrg" as total_charges
from
inpatient_data
order by total_payment desc
limit 10;

-- objective : Utilization metrics by state
-- Reveals the state with high utilization volume and intensity
-- How does average length of stay vary by state, indicating treatment duration

select "Rndrng_Prvdr_Org_Name" as provider_name,
"Rndrng_Prvdr_State_Abrvtn" as states,
sum("Tot_Dschrgs") as total_discharges,
sum("Tot_Cvrd_Days") as total_covered_days,
avg("Tot_Dschrgs") as avg_total_discharges,
avg("Tot_Cvrd_Days") as avg_total_covered_days,
avg("Tot_Dschrgs":: Float / nullif("Tot_Cvrd_Days",0)) as avg_length_of_stay -- "::Float" ensures that result is decimal number and Nullif ensures that if Tot_Dschrgs is 0,dont divide
from
inpatient_data
group by provider_name,states
order by total_discharges desc;

-- High-Cost Areas and Providers

select "Rndrng_Prvdr_State_Abrvtn" as states,
sum("Tot_Pymt_Amt")/nullif(sum("Tot_Dschrgs"),0) as cost_per_discharge,
sum ("Tot_Pymt_Amt")/nullif(sum("Tot_Cvrd_Days"),0) as cost_per_covered_day,
sum("Tot_Pymt_Amt") as total_payment
from
inpatient_data
group by states
order by cost_per_discharge desc;

-- Statistical outlier providers in terms of cost

select "Rndrng_Prvdr_Org_Name" as provider_name,
"Rndrng_Prvdr_State_Abrvtn" as states,
"Tot_Pymt_Amt" as total_payment,
"Bene_Avg_Risk_Scre" as risk_score,
"Tot_Dschrgs" as discharges
from
inpatient_data
where "Tot_Pymt_Amt" > (select avg("Tot_Pymt_Amt")+2* STDDEV("Tot_Pymt_Amt") from inpatient_data)
order by total_payment desc;

-- Chronic Condition Prevalence by State

SELECT 
    "Rndrng_Prvdr_State_Abrvtn" AS state,
    AVG("Bene_CC_PH_Diabetes_V2_Pct") AS avg_diabetes_pct,
    AVG("Bene_CC_PH_HF_NonIHD_V2_Pct") AS avg_heart_failure_pct,
    AVG("Bene_Avg_Risk_Scre") AS avg_risk_score
FROM inpatient_data
GROUP BY "Rndrng_Prvdr_State_Abrvtn"
ORDER BY avg_diabetes_pct DESC;

-- Correlation between chornic conditions and costs

WITH Stats AS (
    SELECT 
        "Bene_CC_PH_Diabetes_V2_Pct" AS diabetes_pct,
        "Bene_CC_PH_HF_NonIHD_V2_Pct" AS heart_failure_pct,
        "Tot_Pymt_Amt" AS total_payment,
        "Tot_Dschrgs" AS discharges
    FROM inpatient_data
)
SELECT 
    CORR(diabetes_pct, total_payment) AS diabetes_cost_corr,
    CORR(heart_failure_pct, total_payment) AS hf_cost_corr,
    CORR(diabetes_pct, discharges) AS diabetes_discharge_corr,
    CORR(heart_failure_pct, discharges) AS hf_discharge_corr
FROM Stats;






