CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__CWT_PATHWAY.CWT_62DAYBREAKDOWN (

    --Description: Further breakdown fields for patients on the 62 Day pathway
    --Author: Jake Kealey

    RECORD_ID VARCHAR,
	D62_ACC_DIAGNOSTIC VARCHAR, --Accountable Diagnostic Provider Code
	D62_ACC_TREATMENT VARCHAR, --Accountable Treatment Provider Code
    D62_ALLOCATIONMETHOD VARCHAR, --Method used to calculate the patient allocation between accountable providers
    D62_6S_SCENARIO NUMBER --If the allocation method is "6 Scenarios", which Scenario was used
)
COMMENT="Breakdown for 62 Day pathway records."
TARGET_LAG = "24 hours"
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT RECORD_ID, D62_ACC_DIAGNOSTIC, D62_ACC_TREATMENT, D62_ALLOCATIONMETHOD, D62_6S_SCENARIO
FROM DEV__MODELLING.CANCER__CWT_PATHWAY.CWT_PERFORMANCE_62DAY
--Logic to prevent multiple rows per record
WHERE D62_ALLOCATIONMETHOD = 'Solo'
OR (
    PER_ORG_TRUST = D62_ACC_DIAGNOSTIC
    AND PER_METRIC = '62 Day')